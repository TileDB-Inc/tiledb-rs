/// Try to derive the pointee type from some common known pointer types
pub fn try_deref(ty: &syn::Type) -> &syn::Type {
    match ty {
        syn::Type::Reference(ref type_ref) => &type_ref.elem,
        syn::Type::Path(ref type_path) => {
            let last = type_path
                .path
                .segments
                .last()
                .expect("Field with empty type path");
            match last.arguments {
                syn::PathArguments::AngleBracketed(ref arg) => {
                    if arg.args.len() == 1 {
                        if matches!(
                            last.ident.to_string().as_ref(),
                            "Box" | "Arc" | "Rc"
                        ) {
                            if let syn::GenericArgument::Type(pointee) =
                                arg.args.first().unwrap()
                            {
                                return try_deref(pointee);
                            }
                        }
                    }
                    ty
                }
                _ => ty,
            }
        }
        _ => ty,
    }
}
