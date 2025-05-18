#[derive(Clone)]
pub struct ConfigActor {
    pub(crate) dir: &'static str,
    pub dbfilename: &'static str,
}

impl ConfigActor {
    pub fn new(dir: String, dbfilename: String) -> Self {
        Self {
            dir: Box::leak(dir.into_boxed_str()),
            dbfilename: Box::leak(dbfilename.into_boxed_str()),
        }
    }

    pub(crate) fn set_dir(&mut self, dir: &str) {
        unsafe {
            // Get the pointer to the str
            let ptr: *const str = self.dir;
            // Recreate the Box from the pointer and drop it
            let _reclaimed_box: Box<str> = Box::from_raw(ptr as *mut str);
            self.dir = Box::leak(dir.into());
        }
    }
    pub(crate) fn set_dbfilename(&mut self, dbfilename: &str) {
        unsafe {
            // Get the pointer to the str
            let ptr: *const str = self.dbfilename;
            // Recreate the Box from the pointer and drop it
            let _reclaimed_box: Box<str> = Box::from_raw(ptr as *mut str);
            self.dbfilename = Box::leak(dbfilename.into());
        }
    }

    pub(crate) fn get_filepath(&self) -> String {
        format!("{}/{}", self.dir, self.dbfilename)
    }
}
