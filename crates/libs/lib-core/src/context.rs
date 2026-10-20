use dashmap::DashMap;
use lazy_static::lazy_static;
use reqwest::ClientBuilder;
use std::time::Duration;
use tokio::runtime::Builder;

#[cfg(windows)]
fn pin_runtime_library() {
    use std::ffi::c_void;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetModuleHandleExW(flags: u32, name: *const u16, module: *mut *mut c_void) -> i32;
    }

    const GET_MODULE_HANDLE_EX_FLAG_PIN: u32 = 0x0000_0001;
    const GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS: u32 = 0x0000_0004;
    let mut module = std::ptr::null_mut();
    // This process-wide runtime keeps native worker threads alive after Lua
    // services close. Its code must remain loaded for the same process lifetime;
    // a Lua VM releasing its library handle must not unmap those worker threads.
    let pinned = unsafe {
        GetModuleHandleExW(
            GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS | GET_MODULE_HANDLE_EX_FLAG_PIN,
            pin_runtime_library as *const () as *const u16,
            &mut module,
        )
    };
    if pinned == 0 {
        panic!(
            "Failed to pin Tokio runtime library: {}",
            std::io::Error::last_os_error()
        );
    }
}

lazy_static! {
    pub static ref CONTEXT: Context = {
        #[cfg(windows)]
        pin_runtime_library();
        let tokio_runtime = Builder::new_multi_thread()
            .worker_threads(4)
            .enable_time()
            .enable_io()
            .build();

        Context {
            http_clients: DashMap::new(),
            tokio_runtime: tokio_runtime.expect("Init tokio runtime failed"),
        }
    };
}

pub struct Context {
    http_clients: DashMap<String, reqwest::Client>,
    pub tokio_runtime: tokio::runtime::Runtime,
}

impl Context {
    pub fn get_http_client(&self, timeout: u64, proxy: &String) -> reqwest::Client {
        let name = format!("{}_{}", timeout, proxy);
        if let Some(client) = self.http_clients.get(&name) {
            return client.clone();
        }

        let builder = ClientBuilder::new()
            .timeout(Duration::from_secs(timeout))
            .use_rustls_tls()
            .tcp_nodelay(true);

        let client = if proxy.is_empty() {
            builder.build().unwrap_or_default()
        } else {
            match reqwest::Proxy::all(proxy) {
                Ok(proxy) => builder.proxy(proxy).build().unwrap_or_default(),
                Err(_) => builder.build().unwrap_or_default(),
            }
        };

        self.http_clients.insert(name.to_string(), client.clone());
        client
    }
}
