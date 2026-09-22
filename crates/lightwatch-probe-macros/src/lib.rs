//! The attribute macros behind `lightwatch-probe`. Nothing here is useful on
//! its own; the documentation lives on the `lightwatch-probe` crate, which
//! re-exports both attributes.

use proc_macro::TokenStream;
use proc_macro2::Span;
use proc_macro_crate::{crate_name, FoundCrate};
use quote::{format_ident, quote};
use syn::parse::Parser as _;
use syn::{parse_macro_input, Ident, ImplItem, Item, ItemFn, ItemImpl, ItemMod, ItemStruct, LitStr};

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
    let mut skip = false;
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("name") {
            name_override = Some(meta.value()?.parse()?);
            Ok(())
        } else if meta.path.is_ident("skip") {
            skip = true;
            Ok(())
        } else {
            Err(meta.error("expected `name = \"...\"` or `skip`"))
        }
    });
    parse_macro_input!(attr with parser);

    let function = parse_macro_input!(item as ItemFn);

    // Written so a function inside a `#[measure_all]` module can opt out in
    // place, next to the reason, rather than by being moved out of it.
    if skip {
        return quote!(#function).into();
    }

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

/// Measures every function in a `mod` or an `impl` block.
///
/// A call graph is only as complete as the functions in it, and reaching a
/// useful node count one attribute at a time is the reason most instrumented
/// programs have three measured functions and no graph worth looking at.
///
/// Unlike [`measure`], this skips what it cannot measure — `async fn`,
/// `const fn`, and anything already carrying `#[measure]` — instead of
/// failing the build. Asking for one function to be measured and being told
/// no is useful; asking for a module and being told no because one function
/// in it is `async` is not. Put `#[measure(skip)]` on a function to leave it
/// out on purpose.
#[proc_macro_attribute]
pub fn measure_all(attr: TokenStream, item: TokenStream) -> TokenStream {
    if !attr.is_empty() {
        let attr: proc_macro2::TokenStream = attr.into();
        return syn::Error::new_spanned(attr, "`measure_all` takes no arguments")
            .to_compile_error()
            .into();
    }

    let item = parse_macro_input!(item as Item);
    match item {
        Item::Mod(module) => expand_measure_all_mod(module),
        Item::Impl(block) => expand_measure_all_impl(block),
        other => syn::Error::new_spanned(
            other,
            "`measure_all` applies to a `mod` or an `impl` block. For one function, use \
             `#[measure]`, which will also tell you when a function cannot be measured.",
        )
        .to_compile_error()
        .into(),
    }
}

fn expand_measure_all_mod(mut module: ItemMod) -> TokenStream {
    if let Some((_, items)) = &mut module.content {
        for item in items.iter_mut() {
            match item {
                Item::Fn(function) => measure_in_place(&mut function.attrs, &function.sig),
                // Only direct children: a nested module says for itself
                // whether it wants measuring, and an `impl` inside one is
                // reached by putting the attribute on the impl.
                _ => {}
            }
        }
    } else {
        return syn::Error::new_spanned(
            module,
            "`measure_all` needs the module's body, so it cannot go on `mod foo;`",
        )
        .to_compile_error()
        .into();
    }
    quote!(#module).into()
}

fn expand_measure_all_impl(mut block: ItemImpl) -> TokenStream {
    for item in block.items.iter_mut() {
        if let ImplItem::Fn(function) = item {
            measure_in_place(&mut function.attrs, &function.sig);
        }
    }
    quote!(#block).into()
}

/// Adds `#[measure]` to a function unless it cannot take one or already has.
fn measure_in_place(attrs: &mut Vec<syn::Attribute>, sig: &syn::Signature) {
    if sig.asyncness.is_some() || sig.constness.is_some() {
        return;
    }
    let already = attrs.iter().any(|attr| attr.path().segments.last().is_some_and(|last| last.ident == "measure"));
    if already {
        return;
    }
    let probe = probe_crate();
    attrs.insert(0, syn::parse_quote!(#[#probe::measure]));
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
