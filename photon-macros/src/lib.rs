//! L4 代码生成：把用户写的服务 trait 展开成「可实现 + 可注册 + 可调用」三件套。
//!
//! 展开产物（同一模块内）：
//!
//! - `mod __photon_<svc>`：`SERVICE_ID`、`METHOD_NAMES`、每个方法一个 `HASH_<METHOD>`
//! - `struct <Svc>Client<S, K>`：持有 `SessionHandle`，方法全是 `async fn`
//! - `struct <Svc>BlockingClient<S, K>`：持有 `BlockingSession`，方法全是同步 `fn`
//!   （只有服务级写了 `blocking` 才生成）
//! - `struct <Svc>Service<S>`：包一层 `Arc<S>` 并实现 `Service`，按 `method` 分流
//! - `fn register_<svc>(registry, svc)`：用同一个 `service_id` 占号
//!
//! 三档声明形态（`async fn` / `fn` / `#[photon(blocking)] fn`）都支持：调度策略在
//! 展开期写死进各自的 dispatch 分支，运行期不做二次判断。

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    Expr, ExprLit, FnArg, GenericArgument, ItemTrait, Lit, Meta, Pat, PathArguments, ReturnType,
    Token, TraitItem, Type, parse_macro_input, parse_quote, punctuated::Punctuated,
};

/// 服务级属性解析结果。
struct ServiceArgs {
    id: u16,
    blocking: bool,
    /// `error = MyError`：业务错误类型；`None` 表示方法直接返回 `RpcError`。
    error: Option<Type>,
}

impl syn::parse::Parse for ServiceArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut id = None;
        let mut blocking = false;
        let mut error = None;

        for meta in Punctuated::<Meta, Token![,]>::parse_terminated(input)? {
            match &meta {
                Meta::NameValue(nv) if nv.path.is_ident("id") => match &nv.value {
                    Expr::Lit(ExprLit {
                        lit: Lit::Int(lit), ..
                    }) => id = Some(lit.base10_parse::<u16>()?),
                    other => {
                        return Err(syn::Error::new_spanned(
                            other,
                            "`id` 必须是 u16 字面量，例如 `id = 7`",
                        ));
                    }
                },
                Meta::Path(path) if path.is_ident("blocking") => blocking = true,
                Meta::NameValue(nv) if nv.path.is_ident("error") => match &nv.value {
                    Expr::Path(expr) => {
                        error = Some(Type::Path(syn::TypePath {
                            qself: None,
                            path: expr.path.clone(),
                        }))
                    }
                    other => {
                        return Err(syn::Error::new_spanned(
                            other,
                            "`error` 必须是类型路径，例如 `error = CalcError`",
                        ));
                    }
                },
                other => {
                    return Err(syn::Error::new_spanned(
                        other,
                        "`#[photon::service(...)]` 只接受 `id = N`、`blocking` 与 `error = Type`",
                    ));
                }
            }
        }

        let id = id.ok_or_else(|| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                "缺少必填参数 `id = N`：service_id 必须显式分配，\
                 自动编号会在不同 crate 各自注册时撞号，且顺序一变线上 id 就变",
            )
        })?;

        Ok(Self {
            id,
            blocking,
            error,
        })
    }
}

/// method id：只哈希方法名，64 位。
///
/// 服务名不进哈希——改 trait 名是最常见的一次重构，那不该等于全量改线上 method id。
fn method_hash(name: &str) -> u64 {
    xxhash_rust::xxh3::xxh3_64(name.as_bytes())
}

/// handler 的执行形态，决定 dispatch 分支怎么包。
enum MethodKind {
    /// `async fn`：本处 await 即可，不换线程。
    Async,
    /// `fn`：包成 async 块，首 poll 就执行完——不换线程、不加调度。
    Sync,
    /// `#[photon(blocking)] fn`：唯一会换线程的分支。
    Blocking,
}

/// 一个 handler 方法的展开素材。
struct Method {
    name: syn::Ident,
    hash: u64,
    kind: MethodKind,
    /// 参数名（dispatch 里要按名字传给用户方法）与类型（decode 的目标）。
    args: Vec<(syn::Ident, Type)>,
    /// `Result<T, E>` 里的 `T`。
    ok_ty: Type,
    /// 显式标了 `#[photon(zero_copy)]`；只有同步 `fn` 才可能为真。
    zero_copy: bool,
}

#[proc_macro_attribute]
pub fn service(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args: ServiceArgs = match syn::parse(attr) {
        Ok(args) => args,
        Err(err) => return err.to_compile_error().into(),
    };
    let mut service = parse_macro_input!(item as ItemTrait);

    let methods = match collect_methods(&service, args.error.as_ref()) {
        Ok(methods) => methods,
        Err(err) => return err.to_compile_error().into(),
    };

    // 方法级 `#[photon(...)]` 只在展开期有意义。留着它，rustc 会去解析一个
    // 不存在的 `#[photon]` 属性并直接报错——信息已经收进 `MethodKind` 了。
    strip_method_attrs(&mut service);

    let svc = &service.ident;
    let client = format_ident!("{}Client", svc);
    let server = format_ident!("{}Service", svc);
    let helper = format_ident!("__photon_{}", snake_case(&svc.to_string()));
    let register = format_ident!("register_{}", snake_case(&svc.to_string()));

    // `async fn` 在 trait 里不保证 future 是 `Send`，而 dispatch 的 `BoxFuture` 要求
    // `Send`。所以把签名改写成带 `+ Send` 的 RPITIT——调用方仍然写 `async fn` 实现，
    // 编译器会替我们验一遍「你这个 impl 的 future 真的是 Send」。
    rewrite_sigs(&mut service);

    let generated = expand(
        &service, &methods, &args, &client, &server, &helper, &register,
    );
    quote!(#service #generated).into()
}

fn expand(
    service: &ItemTrait,
    methods: &[Method],
    args: &ServiceArgs,
    client: &syn::Ident,
    server: &syn::Ident,
    helper: &syn::Ident,
    register: &syn::Ident,
) -> TokenStream2 {
    let svc = &service.ident;
    let service_id = args.id;

    let method_names = methods.iter().map(|m| {
        let hash = hash_literal(m.hash);
        let name = m.name.to_string();
        quote! { (#hash, #name) }
    });
    let hash_consts = methods.iter().map(|m| {
        let ident = hash_ident(&m.name);
        let hash = hash_literal(m.hash);
        quote! { pub(super) const #ident: u64 = #hash; }
    });

    let client_methods = methods.iter().map(|m| {
        let name = &m.name;
        let params = m.args.iter().map(|(ident, ty)| quote! { #ident: #ty });
        let arg_names: Vec<_> = m.args.iter().map(|(ident, _)| ident).collect();
        let arg_types: Vec<_> = m.args.iter().map(|(_, ty)| ty).collect();
        let hash = hash_ident(name);
        let ok_ty = &m.ok_ty;
        // 声明了 `error = E` 时，线上/本地的 `RpcError` 都要还原成业务错误类型。
        let unwrap = match &args.error {
            Some(err_ty) => {
                quote! { .map_err(|e: ::photon::RpcError| <#err_ty as ::photon::ServiceError>::from_error(e)) }
            }
            None => quote! {},
        };
        // 返回错误类型跟着服务级声明走：写了 `error = E`，stub 交出的就是 `E`。
        let ret_err = match &args.error {
            Some(err_ty) => quote! { #err_ty },
            None => quote! { ::photon::RpcError },
        };
        let packed = if arg_types.is_empty() {
            quote! { &() }
        } else {
            quote! { &(#(#arg_names),*) }
        };
        let name_str = m.name.to_string();
        quote! {
            /// 发一次请求；期限取客户端上的 `deadline_ms`（默认 0 = 不设期限）。
            pub async fn #name(&self, #(#params),*)
                -> ::std::result::Result<#ok_ty, #ret_err>
            {
                let payload =
                    <::photon::codec::Default as ::photon::Codec>::encode(#packed) #unwrap?;
                // 一次调用一个 span，`request_id` 在 `Session::call` 里回填——那才
                // 是分配它的地方。`method_name` 是编译期常量，所以客户端这一侧也能
                // 带上名字（服务端靠 `Service::method_name` 拿同一份名字表）。
                let span = ::photon::__private::tracing::info_span!(
                    "photon.call",
                    service_id = #helper::SERVICE_ID,
                    method = #helper::#hash,
                    method_name = #name_str,
                    deadline_ms = self.deadline_ms,
                    request_id = ::photon::__private::tracing::field::Empty,
                );
                let reply = ::photon::__private::instrument(span, self
                    .handle
                    .call(Self::SERVICE_ID, #helper::#hash, payload, self.deadline_ms))
                    .await
                    #unwrap?;
                <::photon::codec::Default as ::photon::Codec>::decode::<#ok_ty>(&reply) #unwrap
            }
        }
    });

    // 同步客户端：只有服务级写了 `blocking` 才生成。
    //
    // 它不做任何「把异步包一层」的伪装：`BlockingSession` 自带线程与 runtime，
    // 这里生成的每个方法只是「encode → 阻塞 call → decode」。失败路径和异步 stub
    // 一样是三处 `from_error`（encode / call / decode），否则业务错误会在半路变回
    // 框架错误。
    let blocking_client = if args.blocking {
        let blocking_ident = format_ident!("{}BlockingClient", svc);
        let blocking_methods = methods.iter().map(|m| {
            let name = &m.name;
            let params = m.args.iter().map(|(ident, ty)| quote! { #ident: #ty });
            let arg_names: Vec<_> = m.args.iter().map(|(ident, _)| ident).collect();
            let arg_types: Vec<_> = m.args.iter().map(|(_, ty)| ty).collect();
            let hash = hash_ident(name);
            let ok_ty = &m.ok_ty;
            let unwrap = match &args.error {
                Some(err_ty) => {
                    quote! { .map_err(|e: ::photon::RpcError| <#err_ty as ::photon::ServiceError>::from_error(e)) }
                }
                None => quote! {},
            };
            let ret_err = match &args.error {
                Some(err_ty) => quote! { #err_ty },
                None => quote! { ::photon::RpcError },
            };
            let packed = if arg_types.is_empty() {
                quote! { &() }
            } else {
                quote! { &(#(#arg_names),*) }
            };
            let name_str = m.name.to_string();
            quote! {
                /// 发一次请求并阻塞等回包；期限取同步客户端上的 `deadline_ms`。
                pub fn #name(&self, #(#params),*)
                    -> ::std::result::Result<#ok_ty, #ret_err>
                {
                    let payload =
                        <::photon::codec::Default as ::photon::Codec>::encode(#packed) #unwrap?;
                    // 名字只进 span（同步路径的 span 在会话线程上建，所以这里把
                    // 编译期常量一起递过去）。语义与 `call` 完全相同。
                    let reply = self
                        .session
                        .call_named(
                            Self::SERVICE_ID,
                            #helper::#hash,
                            ::std::option::Option::Some(#name_str),
                            payload,
                            self.deadline_ms,
                        )
                        #unwrap?;
                    <::photon::codec::Default as ::photon::Codec>::decode::<#ok_ty>(&reply) #unwrap
                }
            }
        });

        quote! {
            /// 该服务的同步客户端：调用方不需要 runtime，也不需要写 `await`。
            ///
            /// 它持有一条 [`::photon::blocking::BlockingSession`]——会话与 runtime
            /// 都在自己的线程上，所以「在 `main` 里调用」「在 `#[tokio::test]` 里
            /// 调用」都不会 panic。代价是每个实例多三条线程。
            pub struct #blocking_ident<S, K>
            where
                S: ::photon::transport::MessageSource,
                K: ::photon::transport::MessageSink,
                S: 'static,
                K: 'static,
            {
                session: ::photon::blocking::BlockingSession<S, K>,
                /// 这个客户端上所有请求的期限（毫秒，0 = 不设期限）。
                deadline_ms: u32,
            }

            impl<S, K> #blocking_ident<S, K>
            where
                S: ::photon::transport::MessageSource + 'static,
                K: ::photon::transport::MessageSink + 'static,
            {
                pub const SERVICE_ID: u16 = #helper::SERVICE_ID;

                pub fn new(session: ::photon::blocking::BlockingSession<S, K>) -> Self {
                    Self {
                        session,
                        deadline_ms: 0,
                    }
                }

                /// 设协议头里的期限（服务端到点放弃执行），返回新的客户端。
                pub fn with_deadline(self, deadline_ms: u32) -> Self {
                    Self {
                        deadline_ms,
                        ..self
                    }
                }

                /// 设**本地**等待上限：对端不回时保护调用方这一侧。
                ///
                /// 与 `with_deadline` 是两件事：期限进协议头、跨进程有效；这个只在
                /// 本进程内计时，用来兜住僵死连接。两者可以同时设。
                pub fn with_timeout(self, timeout: ::std::time::Duration) -> Self {
                    Self {
                        session: self.session.with_timeout(timeout),
                        ..self
                    }
                }

                /// 当前期限（毫秒，0 = 不设期限）。
                pub fn deadline_ms(&self) -> u32 {
                    self.deadline_ms
                }

                /// 当前本地等待上限（`None` = 无限等）。
                pub fn timeout(&self) -> ::std::option::Option<::std::time::Duration> {
                    self.session.timeout()
                }

                pub fn session(&self) -> &::photon::blocking::BlockingSession<S, K> {
                    &self.session
                }

                #(#blocking_methods)*
            }
        }
    } else {
        quote! {}
    };

    let dispatch_arms = methods.iter().map(|m| {
        let hash = hash_ident(&m.name);
        let name = &m.name;
        let arg_names: Vec<_> = m.args.iter().map(|(ident, _)| ident).collect();
        let arg_types: Vec<_> = m.args.iter().map(|(_, ty)| ty).collect();
        let decode = if arg_types.is_empty() {
            quote! { let (): () = (); }
        } else {
            quote! {
                let (#(#arg_names),*): (#(#arg_types),*) =
                    <::photon::codec::Default as ::photon::Codec>::decode(&payload)?;
            }
        };
        let call = quote! { <S as #svc>::#name(&*me, #(#arg_names),*) };
        // 业务错误在进入 `Response` 之前就换成 `RpcError`，断言放在这里：
        // 它是唯一知道「这个 error 来自用户 handler」的位置。
        let wrap = match &args.error {
            Some(err_ty) => quote! {
                .map_err(|e| {
                    let err = <#err_ty as ::photon::ServiceError>::into_error(e);
                    ::photon::error::assert_business_error(&err);
                    err
                })
            },
            None => quote! {},
        };
        let body = match m.kind {
            MethodKind::Async => quote! {
                #decode
                let out = #call.await #wrap?;
                <::photon::codec::Default as ::photon::Codec>::encode(&out)
            },
            // 同步 `fn` 就地执行：它承诺的是「服务端同步完成」，所以包成 async 块
            // 之后首 poll 就跑完，不换线程、也不产生额外的调度点。
            MethodKind::Sync => quote! {
                #decode
                let out = #call #wrap?;
                <::photon::codec::Default as ::photon::Codec>::encode(&out)
            },
            // 唯一换线程的分支。decode 也搬进闭包：它同样是 CPU 工作，放在 worker 上
            // 等于把省下来的阻塞又还回去。
            MethodKind::Blocking => quote! {
                let me = ::std::sync::Arc::clone(&me);
                ::tokio::task::spawn_blocking(move || {
                    #decode
                    let out = #call #wrap?;
                    <::photon::codec::Default as ::photon::Codec>::encode(&out)
                })
                .await
                .map_err(|join| {
                    ::photon::RpcError::Internal(format!("blocking handler 未正常结束: {join}"))
                })?
            },
        };
        quote! {
            #helper::#hash => { #body }
        }
    });

    // 无 blocking 方法时干脆不生成 `schedule`，让 trait 的默认实现接管：省掉一个
    // 永远返回同一个值的 match。
    let blocking_hashes = methods
        .iter()
        .filter(|m| matches!(m.kind, MethodKind::Blocking))
        .map(|m| {
            let hash = hash_ident(&m.name);
            quote! { #helper::#hash }
        })
        .collect::<Vec<_>>();
    let zero_copy_hashes = methods
        .iter()
        .filter(|m| m.zero_copy)
        .map(|m| {
            let hash = hash_ident(&m.name);
            quote! { #helper::#hash }
        })
        .collect::<Vec<_>>();
    let payload_mode_impl = if zero_copy_hashes.is_empty() {
        quote! {}
    } else {
        quote! {
            fn payload_mode(&self, method: u64) -> ::photon::PayloadMode {
                match method {
                    #(#zero_copy_hashes)|* => ::photon::PayloadMode::ZeroCopy,
                    _ => ::photon::PayloadMode::Copy,
                }
            }
        }
    };
    let schedule_impl = if blocking_hashes.is_empty() {
        quote! {}
    } else {
        quote! {
            fn schedule(&self, method: u64) -> ::photon::Schedule {
                match method {
                    #(#blocking_hashes)|* => ::photon::Schedule::Blocking,
                    _ => ::photon::Schedule::Async,
                }
            }
        }
    };

    // 名字表交给 L3：method id 是哈希，只有这里反推得出名字。
    let method_name_impl = quote! {
        fn method_name(&self, method: u64) -> ::std::option::Option<&'static str> {
            #helper::METHOD_NAMES
                .iter()
                .find(|(hash, _)| *hash == method)
                .map(|(_, name)| *name)
        }
    };

    quote! {
        #[doc(hidden)]
        mod #helper {
            pub(super) const SERVICE_ID: u16 = #service_id;
            /// 方法名表：请求 span、`UnknownMethod` 日志、panic 报告都靠它。
            pub(super) const METHOD_NAMES: &[(u64, &str)] = &[#(#method_names),*];
            #(#hash_consts)*
        }

        /// 该服务的异步客户端。线上永远异步，与 handler 声明形态无关。
        ///
        /// 只泛型化到「传输半身」而不是 `Transport`：进程内配对（`Memory::pair`）
        /// 不实现 `Transport`，绑在它上面会让同一条链路的两端用不上同一份 stub。
        /// 传输相关的入口是 `photon::connect::<T>()`，拿到句柄再 `new` 进来。
        pub struct #client<S, K>
        where
            S: ::photon::transport::MessageSource,
            K: ::photon::transport::MessageSink,
            S: 'static,
            K: 'static,
        {
            handle: ::photon::session::SessionHandle<S, K>,
            /// 这个客户端上所有请求的期限（毫秒，0 = 不设期限）。
            deadline_ms: u32,
        }

        impl<S, K> #client<S, K>
        where
            S: ::photon::transport::MessageSource + 'static,
            K: ::photon::transport::MessageSink + 'static,
        {
            /// 与 `register_*` 用的是同一个 `service_id`：一处写、两处用。
            pub const SERVICE_ID: u16 = #helper::SERVICE_ID;

            pub fn new(handle: ::photon::session::SessionHandle<S, K>) -> Self {
                Self {
                    handle,
                    deadline_ms: 0,
                }
            }

            /// 给这个客户端上的请求设一个期限，返回新的客户端（原值不变）。
            ///
            /// 用「换一个值」而不是「改全局状态」：期限是每个调用点的选择，
            /// 让 `&self` 上挂着可变期限会让并发调用互相影响。
            pub fn with_deadline(self, deadline_ms: u32) -> Self {
                Self {
                    deadline_ms,
                    ..self
                }
            }

            /// 当前期限（毫秒，0 = 不设期限）。
            pub fn deadline_ms(&self) -> u32 {
                self.deadline_ms
            }

            pub fn handle(&self) -> &::photon::session::SessionHandle<S, K> {
                &self.handle
            }

            #(#client_methods)*
        }

        #blocking_client

        /// 把一个实现类型的实例变成可注册的服务。
        ///
        /// 生成的 `impl Service` 只能落在**本 crate 内的类型**上：`Service` 来自
        /// photon，裸类型参数直接实现过不了孤儿规则。包一层本地类型既合规，
        /// 也让同一实现类型可以同时实现多个服务 trait（不再是一类型一服务）。
        pub struct #server<S>
        where
            S: #svc + Send + Sync + 'static,
        {
            inner: ::std::sync::Arc<S>,
        }

        impl<S> #server<S>
        where
            S: #svc + Send + Sync + 'static,
        {
            pub fn new(inner: ::std::sync::Arc<S>) -> Self {
                Self { inner }
            }

            pub fn inner(&self) -> &::std::sync::Arc<S> {
                &self.inner
            }
        }

        impl<S> ::std::clone::Clone for #server<S>
        where
            S: #svc + Send + Sync + 'static,
        {
            fn clone(&self) -> Self {
                Self {
                    inner: ::std::sync::Arc::clone(&self.inner),
                }
            }
        }

        /// dispatch：`match method` 的每个键都是编译期常量，运行期只剩一次比较链。
        impl<S> ::photon::Service for #server<S>
        where
            S: #svc + Send + Sync + 'static,
        {
            fn call(
                self: ::std::sync::Arc<Self>,
                method: u64,
                payload: ::photon::Bytes,
            ) -> ::photon::BoxFuture<'static, ::std::result::Result<::photon::Bytes, ::photon::RpcError>>
            {
                ::std::boxed::Box::pin(async move {
                    // 内层 `Arc<S>` 单独拎出来：dispatch 只在 L3 spawn 一次，这里不再分层。
                    let me = ::std::sync::Arc::clone(&self.inner);
                    match method {
                        #(#dispatch_arms)*
                        // 帧格式没问题，是方法没注册：回包侧构造 `Remote{3}` 是允许的。
                        _ => Err(::photon::RpcError::Remote(::photon::RemoteError::new(3))),
                    }
                })
            }

            #schedule_impl
            #payload_mode_impl
            #method_name_impl
        }

        /// 用本服务的 `service_id` 占号。撞号返回 `Err`，不覆盖已有服务。
        pub fn #register<S>(
            registry: &mut ::photon::ServiceRegistry,
            service: ::std::sync::Arc<S>,
        ) -> ::std::result::Result<(), ::photon::RpcError>
        where
            S: #svc + Send + Sync + 'static,
        {
            let wrapped: ::std::sync::Arc<dyn ::photon::Service> =
                ::std::sync::Arc::new(#server::new(service));
            registry.insert(#helper::SERVICE_ID, wrapped)
        }
    }
}

/// 摘掉方法级 `#[photon(...)]`：它的语义已经落进 `MethodKind`。
fn strip_method_attrs(service: &mut syn::ItemTrait) {
    for item in &mut service.items {
        if let TraitItem::Fn(item_fn) = item {
            item_fn.attrs.retain(|attr| !attr.path().is_ident("photon"));
        }
    }
}

/// 把 `async fn x(&self) -> R` 改写成
/// `fn x(&self) -> impl Future<Output = R> + Send + '_`。
fn rewrite_sigs(service: &mut ItemTrait) {
    for item in &mut service.items {
        let TraitItem::Fn(item_fn) = item else {
            continue;
        };
        let sig = &mut item_fn.sig;
        // 同步 `fn` 保持原样：它本来就没有 future 可改写，dispatch 直接就地调用。
        if sig.asyncness.is_none() {
            continue;
        }
        let ReturnType::Type(_, ok_ty) = &sig.output else {
            continue;
        };
        let ok_ty = ok_ty.clone();
        sig.asyncness = None;
        sig.output = parse_quote! {
            -> impl ::std::future::Future<Output = #ok_ty> + ::std::marker::Send + '_
        };
    }
}

fn collect_methods(service: &ItemTrait, declared_error: Option<&Type>) -> syn::Result<Vec<Method>> {
    let mut methods: Vec<Method> = Vec::new();

    for item in &service.items {
        let TraitItem::Fn(item_fn) = item else {
            return Err(syn::Error::new_spanned(
                item,
                "服务 trait 里只允许 `async fn` 方法（M2 不支持关联类型、关联常量以外的项）",
            ));
        };

        // 方法级属性只认 `blocking`；`zero_copy` 属于后续阶段，写出来要报错而不是
        // 静默忽略——静默忽略等于让用户以为它生效了。
        let mut blocking = false;
        let mut zero_copy = false;
        for attr in &item_fn.attrs {
            if !attr.path().is_ident("photon") {
                continue;
            }
            let metas = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
            for meta in metas {
                match &meta {
                    Meta::Path(path) if path.is_ident("blocking") => blocking = true,
                    Meta::Path(path) if path.is_ident("zero_copy") => zero_copy = true,
                    other => {
                        return Err(syn::Error::new_spanned(
                            other,
                            "方法级 `#[photon(...)]` 只接受 `blocking` 与 `zero_copy`",
                        ));
                    }
                }
            }
        }
        let sig = &item_fn.sig;
        let kind = match (sig.asyncness.is_some(), blocking) {
            (true, false) => MethodKind::Async,
            (true, true) => {
                return Err(syn::Error::new_spanned(
                    sig,
                    "`#[photon(blocking)]` 只能标在同步 `fn` 上：`async fn` 本身就是非阻塞的，\
                     挪进 blocking 线程池只会白白占一个线程",
                ));
            }
            (false, false) => MethodKind::Sync,
            (false, true) => MethodKind::Blocking,
        };

        // `zero_copy` 只对同步 `fn` 开放：跨 `await` 的 handler 会把 shm 槽位攥上
        // 不可预期的时间，环被填满后整条通道停摆。blocking 方法同样排除——它在
        // 池线程上一次性跑完，但时长由用户计算量决定，同样不可预期。
        if zero_copy && !matches!(kind, MethodKind::Sync) {
            let reason = match kind {
                MethodKind::Async => {
                    "`zero_copy` 只能标在同步 `fn` 上：async handler 跨 await 拿着 payload，\
                     会把 shm 槽位攥上不可预期的时间"
                }
                MethodKind::Blocking => {
                    "`zero_copy` 不能标在 `#[photon(blocking)]` 方法上：它在池线程上执行，\
                     占用时长由用户计算量决定，不可预期"
                }
                MethodKind::Sync => unreachable!(),
            };
            return Err(syn::Error::new_spanned(sig, reason));
        }

        let mut inputs = sig.inputs.iter();
        match inputs.next() {
            Some(FnArg::Receiver(receiver))
                if receiver.reference.is_some() && receiver.mutability.is_none() => {}
            _ => {
                return Err(syn::Error::new_spanned(
                    &sig.inputs,
                    "handler 的首参必须是 `&self`（不是 `self` / `&mut self` / 无接收者）",
                ));
            }
        }

        let mut args = Vec::new();
        for input in inputs {
            let FnArg::Typed(pat_ty) = input else {
                return Err(syn::Error::new_spanned(input, "只支持 `&self` 一个接收者"));
            };
            let Pat::Ident(pat_ident) = &*pat_ty.pat else {
                return Err(syn::Error::new_spanned(
                    &pat_ty.pat,
                    "handler 参数必须是简单标识符（`a: i64`），不能是模式解构",
                ));
            };
            args.push((pat_ident.ident.clone(), (*pat_ty.ty).clone()));
        }

        let (ok_ty, err_ty) = result_types(&sig.output)?;
        check_error_type(&err_ty, declared_error, sig)?;

        let hash = method_hash(&sig.ident.to_string());
        if let Some(dup) = methods.iter().find(|m| m.hash == hash) {
            return Err(syn::Error::new_spanned(
                sig,
                format!(
                    "方法 `{}` 与 `{}` 的 method id 相同（{}），改名或换方法名",
                    sig.ident, dup.name, hash
                ),
            ));
        }

        methods.push(Method {
            name: sig.ident.clone(),
            hash,
            kind,
            args,
            ok_ty,
            zero_copy,
        });
    }

    if methods.is_empty() {
        return Err(syn::Error::new_spanned(
            service,
            "服务 trait 至少需要一个 handler，否则注册进去也没有可调用的方法",
        ));
    }

    Ok(methods)
}

/// 从 `-> Result<T, E>` 里取 `(T, E)`。
fn result_types(output: &ReturnType) -> syn::Result<(Type, Type)> {
    let ReturnType::Type(_, ty) = output else {
        return Err(syn::Error::new_spanned(
            output,
            "handler 必须返回 `Result<T, RpcError>`",
        ));
    };
    let Type::Path(path) = &**ty else {
        return Err(syn::Error::new_spanned(
            ty,
            "handler 必须返回 `Result<T, RpcError>`",
        ));
    };
    let Some(last) = path.path.segments.last() else {
        return Err(syn::Error::new_spanned(
            ty,
            "handler 必须返回 `Result<T, RpcError>`",
        ));
    };
    if last.ident != "Result" {
        return Err(syn::Error::new_spanned(
            ty,
            "handler 必须返回 `Result<T, RpcError>`",
        ));
    }
    let PathArguments::AngleBracketed(generics) = &last.arguments else {
        return Err(syn::Error::new_spanned(ty, "`Result` 上必须写出类型参数"));
    };
    let Some(GenericArgument::Type(ok_ty)) = generics.args.first() else {
        return Err(syn::Error::new_spanned(
            ty,
            "`Result` 的第一个参数必须是成功类型",
        ));
    };
    let Some(GenericArgument::Type(err_ty)) = generics.args.iter().nth(1) else {
        return Err(syn::Error::new_spanned(
            ty,
            "`Result` 必须写出第二个参数（错误类型）",
        ));
    };
    Ok((ok_ty.clone(), err_ty.clone()))
}

/// 方法返回的错误类型必须与服务级声明一致。
///
/// 没写 `error = ...` 时要求 `RpcError`：否则 handler 的 `?` 会把一个框架不认识的
/// 类型扔给 dispatch，报错发生在展开产物里，用户看到的是「某某 trait 没实现」。
fn check_error_type(
    err_ty: &Type,
    declared: Option<&Type>,
    sig: &syn::Signature,
) -> syn::Result<()> {
    let normalize = |ty: &Type| {
        let tokens = quote!(#ty).to_string();
        tokens.replace(' ', "")
    };
    let last_ident = |ty: &Type| match ty {
        Type::Path(path) => path.path.segments.last().map(|seg| seg.ident.to_string()),
        _ => None,
    };

    match declared {
        Some(declared) => {
            if normalize(err_ty) != normalize(declared) {
                return Err(syn::Error::new_spanned(
                    sig,
                    "方法返回的错误类型必须与服务级 `error = ...` 一致",
                ));
            }
        }
        None => {
            if last_ident(err_ty).as_deref() != Some("RpcError") {
                return Err(syn::Error::new_spanned(
                    sig,
                    "未声明 `error = Type` 时，handler 必须返回 `Result<T, RpcError>`",
                ));
            }
        }
    }
    Ok(())
}

fn hash_ident(name: &syn::Ident) -> syn::Ident {
    format_ident!("HASH_{}", name.to_string().to_uppercase())
}

fn hash_literal(hash: u64) -> proc_macro2::Literal {
    proc_macro2::Literal::u64_suffixed(hash)
}

/// `Calc` → `calc`、`CalcService` → `calc_service`。
fn snake_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for (index, ch) in name.chars().enumerate() {
        if ch.is_uppercase() {
            if index != 0 {
                out.push('_');
            }
            out.extend(ch.to_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn golden_method_id_for_add() {
        // 这条是**冻结向量**：一旦有人「优化」哈希实现（换 xxh3 版本、改喂进去的
        // 字节布局），全部线上 method id 会静默改变，而这个断言会立刻红。
        assert_eq!(method_hash("add"), 0xffb3_52e1_3fd2_8e80_u64);
    }

    #[test]
    fn method_id_depends_on_name_only() {
        assert_eq!(method_hash("add"), method_hash("add"));
        assert_ne!(method_hash("add"), method_hash("sub"));
    }

    #[test]
    fn snake_case_handles_camel() {
        assert_eq!(snake_case("Calc"), "calc");
        assert_eq!(snake_case("CalcService"), "calc_service");
    }
}
