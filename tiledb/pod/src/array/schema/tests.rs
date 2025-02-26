use std::collections::HashMap;

use proptest::prelude::*;

use super::*;

fn instance_schema_get_enumeration(s: SchemaData) {
    for edata in s.enumerations.iter() {
        let lookup =
            s.enumeration(EnumerationKey::EnumerationName(&edata.name));
        assert_eq!(Some(edata), lookup)
    }

    let enumerations = s
        .enumerations
        .iter()
        .map(|e| (e.name.to_owned(), e.clone()))
        .collect::<HashMap<_, _>>();

    for adata in s.attributes.iter() {
        if let Some(ename) = adata.enumeration.as_ref() {
            let edata = enumerations.get(ename).unwrap();
            let lookup =
                s.enumeration(EnumerationKey::AttributeName(&adata.name));
            assert_eq!(Some(edata), lookup)
        }
    }
}

proptest! {
    #[test]
    fn proptest_schema_get_enumeration(s in any::<SchemaData>()) {
        instance_schema_get_enumeration(s)
    }
}
