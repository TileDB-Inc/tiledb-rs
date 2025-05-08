use tiledb_sys2::filter_list;

pub struct FilterList {
    list: cxx::SharedPtr<filter_list::FilterList>,
}

impl FilterList {
    pub(crate) fn new(list: cxx::SharedPtr<filter_list::FilterList>) -> Self {
        Self { list }
    }
}
