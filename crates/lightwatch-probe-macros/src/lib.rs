//! The attribute macros behind `lightwatch-probe`. Nothing here is useful on
//! its own; the documentation lives on the `lightwatch-probe` crate, which
//! re-exports both attributes.

use proc_macro::TokenStream;
use proc_macro2::Span;
use proc_macro_crate::{crate_name, FoundCrate};
use quote::{format_ident, quote};
use syn::parse::Parser as _;
use syn::{parse_macro_input, Ident, ItemFn, ItemStruct, LitStr};

/// The path the generated code calls the probe crate by. A consumer that
/// renames the dependency (`lightwatch = { package = "lightwatch-probe" }`, so
/// the attributes read `#[lightwatch::measure]`) would otherwise get code
/// referring to a crate name that is not in scope.
fn probe_crate() -> proc_macro2::TokenStream {
    match crate_name("lightwatch-probe") {
        Ok(FoundCrate::Name(name)) => {
            let ident = Ident::new(&name, Span::call_site());
            quote!(::#ident)
        }
        // Itself, or a manifest we could not read: the probe crate declares
        // `extern crate self as lightwatch_probe`, so this resolves either way.
        _ => quote!(::lightwatch_probe),
    }
}

/// Records this function's caller, its call count and its wall duration.
///
/// Takes an optional `name = "..."` to give the function a distinct label,
/// which two trait implementations of the same method name need.
#[proc_macro_attribute]
pub fn measure(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut name_override: Option<LitStr> = None;
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("name") {
            name_override = Some(meta.value()?.parse()?);
            Ok(())
        } else {
            Err(meta.error("expected `name = \"...\"`"))
        }
    });
    parse_macro_input!(attr with parser);

    let function = parse_macro_input!(item as ItemFn);

    // An async fn's body becomes a future that a runtime may poll on one
    // thread and resume on another. A thread-local activation stack cannot
    // follow that, and the edges it would record are not merely imprecise,
    // they name the wrong caller. Refusing is the only honest option.
    if let Some(asyncness) = function.sig.asyncness {
        return syn::Error::new_spanned(
            asyncness,
            "lightwatch cannot measure an `async fn`: a future resumed on another thread would \
             be attributed to whatever that thread was doing. Measure the synchronous body \
             instead, by moving it into a plain fn that the async fn calls.",
        )
        .to_compile_error()
        .into();
    }
    if let Some(constness) = function.sig.constness {
        return syn::Error::new_spanned(
            constness,
            "lightwatch cannot measure a `const fn`: measuring needs a clock and a thread-local \
             stack, neither of which exists during const evaluation.",
        )
        .to_compile_error()
        .into();
    }

    expand_measure(function, name_override)
}

#[cfg(not(feature = "enabled"))]
fn expand_measure(function: ItemFn, _name_override: Option<LitStr>) -> TokenStream {
    quote!(#function).into()
}

#[cfg(feature = "enabled")]
fn expand_measure(function: ItemFn, name_override: Option<LitStr>) -> TokenStream {
    let probe = probe_crate();
    let ItemFn { attrs, vis, sig, block } = function;
    let name = name_override
        .unwrap_or_else(|| LitStr::new(&sig.ident.to_string(), sig.ident.span()));

    // The original block becomes the tail expression of the new one, so
    // `return`, `?` and a panic all leave through the guard's Drop and the
    // activation stack stays balanced.
    quote! {
        #(#attrs)*
        #vis #sig {
            static __LIGHTWATCH_SITE: #probe::Site =
                #probe::Site::new(#name, module_path!(), file!(), line!());
            let __lightwatch_activation = #probe::Site::enter(&__LIGHTWATCH_SITE);
            #block
        }
    }
    .into()
}

/// Counts live instances of a struct and their sizes.
///
/// Injects a private census field, so an instance is built through the
/// generated `new_tracked` constructor rather than a struct literal. The
/// constructor takes a generated `{Ident}Fields` struct rather than a
/// parameter per field: converting a literal then keeps every field written
/// by name, and two same-typed neighbours like `width` and `height` cannot
/// swap on the way through.
///
/// Pass `manual_measured` when the type implements
/// `lightwatch::Measured` itself, which any type owning a heap buffer should.
#[proc_macro_attribute]
pub fn track(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut manual_measured = false;
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("manual_measured") {
            manual_measured = true;
            Ok(())
        } else {
            Err(meta.error("expected `manual_measured`"))
        }
    });
    parse_macro_input!(attr with parser);

    let item = parse_macro_input!(item as ItemStruct);
    match expand_track(item, manual_measured) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

fn expand_track(
    mut item: ItemStruct,
    manual_measured: bool,
) -> syn::Result<proc_macro2::TokenStream> {
    let probe = probe_crate();

    if !item.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &item.generics,
            "lightwatch cannot track a generic struct: its counters live in one `static` per \
             type, which every instantiation would share.",
        ));
    }

    let ident = item.ident.clone();
    let vis = item.vis.clone();
    let name = LitStr::new(&ident.to_string(), ident.span());
    let census_field = format_ident!("__lightwatch_census");
    let fields_ident = format_ident!("{ident}Fields");
    let fields_doc = LitStr::new(
        &format!("Every field of [`{ident}`] except the census one."),
        ident.span(),
    );

    // Read before the census field is pushed, so the companion carries the
    // author's fields and not the one the attribute adds.
    let declared: Vec<syn::Field> = {
        let syn::Fields::Named(fields) = &mut item.fields else {
            return Err(syn::Error::new_spanned(
                &item,
                "lightwatch can only track a struct with named fields, because it injects one.",
            ));
        };
        let declared = fields.named.iter().cloned().collect();
        fields.named.push(syn::Field::parse_named.parse2(quote! {
            #[doc(hidden)]
            #[allow(dead_code)]
            #census_field: #probe::Census<#ident>
        })?);
        declared
    };
    let declarations = declared.iter().map(|field| {
        let (vis, name, ty) = (&field.vis, &field.ident, &field.ty);
        quote!(#vis #name: #ty)
    });
    let names: Vec<&syn::Ident> = declared
        .iter()
        .map(|field| field.ident.as_ref().expect("named fields"))
        .collect();

    let measured = (!manual_measured).then(|| {
        quote! {
            // Useless for a type that owns heap. See the crate documentation.
            impl #probe::Measured for #ident {
                fn bytes(&self) -> usize {
                    ::core::mem::size_of::<Self>()
                }
            }
        }
    });

    Ok(quote! {
        #item

        #[doc = #fields_doc]
        #vis struct #fields_ident {
            #(#declarations,)*
        }

        impl #probe::Tracked for #ident {
            fn slot() -> &'static #probe::TypeSlot {
                static SLOT: #probe::TypeSlot = #probe::TypeSlot::new(#name, file!(), line!());
                &SLOT
            }
        }

        #measured

        impl #ident {
            /// Builds a counted instance.
            ///
            /// The census field is filled in after every other field, so the
            /// size it records is this instance's real
            /// `lightwatch::Measured::bytes`, heap included.
            #[allow(dead_code)]
            #vis fn new_tracked(fields: #fields_ident) -> Self {
                let #fields_ident { #(#names,)* } = fields;
                let mut value = #ident {
                    #(#names,)*
                    #census_field: #probe::Census::UNCOUNTED,
                };
                let counted = #probe::Census::measuring(&value);
                value.#census_field = counted;
                value
            }
        }
    })
}
