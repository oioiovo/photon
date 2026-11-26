//! L4 代码生成：把用户写的服务 trait 展开成「可实现 + 可注册 + 可调用」三件套。
//!
//! 展开产物（同一模块内）：
//!
//! - `mod __photon_<svc>`：`SERVICE_ID`、`METHOD_NAMES`、每个方法一个 `HASH_<METHOD>`
//! - `struct <Svc>Client<T>`：持有 `SessionHandle`，方法全是 `async fn`
//! - `impl<S: <Svc>> Service for S`：按 `method` 分流的 dispatch
//! - `fn register_<svc>(registry, svc)`：用同一个 `service_id` 占号
//!
//! M2 只支持 `async fn` 形态：`fn` 与 `#[photon(blocking)]` 留到 M3，遇到就直接
//! 报编译错误，不做静默降级。

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
}

impl syn::parse::Parse for ServiceArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut id = None;
        let mut blocking = false;

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
                other => {
                    return Err(syn::Error::new_spanned(
                        other,
                        "`#[photon::service(...)]` 只接受 `id = N` 与 `blocking`",
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

        Ok(Self { id, blocking })
    }
}

/// method id：只哈希方法名，64 位。
///
/// 服务名不进哈希——改 trait 名是最常见的一次重构，那不该等于全量改线上 method id。
fn method_hash(name: &str) -> u64 {
    xxhash_rust::xxh3::xxh3_64(name.as_bytes())
}

/// 一个 handler 方法的展开素材。
struct Method {
    name: syn::Ident,
    hash: u64,
    /// 参数名（dispatch 里要按名字传给用户方法）与类型（decode 的目标）。
    args: Vec<(syn::Ident, Type)>,
    /// `Result<T, RpcError>` 里的 `T`。
    ok_ty: Type,
}

#[proc_macro_attribute]
pub fn service(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args: ServiceArgs = match syn::parse(attr) {
        Ok(args) => args,
        Err(err) => return err.to_compile_error().into(),
    };
    let mut service = parse_macro_input!(item as ItemTrait);

    if args.blocking {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "`blocking` 参数要生成的同步客户端属于 M3：它需要自建 runtime + \
             内外两层超时，现在生成出来只是一段没人验证过的代码",
        )
        .to_compile_error()
        .into();
    }

    let methods = match collect_methods(&service) {
        Ok(methods) => methods,
        Err(err) => return err.to_compile_error().into(),
    };

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
        let packed = if arg_types.is_empty() {
            quote! { &() }
        } else {
            quote! { &(#(#arg_names),*) }
        };
        quote! {
            /// 发一次请求。期限为 0 表示不设期限。
            pub async fn #name(&self, #(#params),*)
                -> ::std::result::Result<#ok_ty, ::photon::RpcError>
            {
                let payload = <::photon::codec::Default as ::photon::Codec>::encode(#packed)?;
                let reply = self
                    .handle
                    .call(Self::SERVICE_ID, #helper::#hash, payload, 0)
                    .await?;
                <::photon::codec::Default as ::photon::Codec>::decode::<#ok_ty>(&reply)
            }
        }
    });

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
        quote! {
            #helper::#hash => {
                #decode
                let out = <S as #svc>::#name(&*me, #(#arg_names),*).await?;
                <::photon::codec::Default as ::photon::Codec>::encode(&out)
            }
        }
    });

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
        }

        impl<S, K> #client<S, K>
        where
            S: ::photon::transport::MessageSource + 'static,
            K: ::photon::transport::MessageSink + 'static,
        {
            /// 与 `register_*` 用的是同一个 `service_id`：一处写、两处用。
            pub const SERVICE_ID: u16 = #helper::SERVICE_ID;

            pub fn new(handle: ::photon::session::SessionHandle<S, K>) -> Self {
                Self { handle }
            }

            pub fn handle(&self) -> &::photon::session::SessionHandle<S, K> {
                &self.handle
            }

            #(#client_methods)*
        }

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

/// 把 `async fn x(&self) -> R` 改写成
/// `fn x(&self) -> impl Future<Output = R> + Send + '_`。
fn rewrite_sigs(service: &mut ItemTrait) {
    for item in &mut service.items {
        let TraitItem::Fn(item_fn) = item else {
            continue;
        };
        let sig = &mut item_fn.sig;
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

fn collect_methods(service: &ItemTrait) -> syn::Result<Vec<Method>> {
    let mut methods: Vec<Method> = Vec::new();

    for item in &service.items {
        let TraitItem::Fn(item_fn) = item else {
            return Err(syn::Error::new_spanned(
                item,
                "服务 trait 里只允许 `async fn` 方法（M2 不支持关联类型、关联常量以外的项）",
            ));
        };

        for attr in &item_fn.attrs {
            if attr.path().is_ident("photon") {
                return Err(syn::Error::new_spanned(
                    attr,
                    "方法级 `#[photon(...)]`（`blocking` / `zero_copy`）属于 M3；M2 只支持 `async fn`",
                ));
            }
        }

        let sig = &item_fn.sig;
        if sig.asyncness.is_none() {
            return Err(syn::Error::new_spanned(
                sig,
                "M2 只支持 `async fn` 形态；同步 `fn` 与 `#[photon(blocking)]` 留到 M3",
            ));
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

        let ok_ty = result_ok_type(&sig.output)?;

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
            args,
            ok_ty,
        });
    }

    if methods.is_empty() {
        return Err(syn::Error::new_spanned(
            service,
            "服务 trait 至少需要一个 `async fn` handler，否则注册进去也没有可调用的方法",
        ));
    }

    Ok(methods)
}

/// 从 `-> Result<T, RpcError>` 里取 `T`。
fn result_ok_type(output: &ReturnType) -> syn::Result<Type> {
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
    match generics.args.first() {
        Some(GenericArgument::Type(ok_ty)) => Ok(ok_ty.clone()),
        _ => Err(syn::Error::new_spanned(
            ty,
            "`Result` 的第一个参数必须是成功类型",
        )),
    }
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
