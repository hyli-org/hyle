use std::{
    any::type_name,
    fs,
    future::Future,
    io::{BufWriter, Write},
    path::Path,
    pin::Pin,
    time::Duration,
};

use crate::{
    bus::{BusClientReceiver, BusClientSender, SharedMessageBus},
    bus_client, handle_messages, log_error,
};
use anyhow::{bail, Error, Result};
use axum::Router;
use rand::{distributions::Alphanumeric, Rng};
use tokio::task::JoinHandle;
use tracing::{debug, info};

const MODULE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

pub mod admin;
pub mod bus_ws_connector;
pub mod contract_state_indexer;
pub mod da_listener;
pub mod prover;
pub mod prover_metrics;
pub mod rest;
pub mod signed_da_listener;
pub mod websocket;

#[derive(Default)]
pub struct BuildApiContextInner {
    pub router: std::sync::Mutex<Option<Router>>,
    pub openapi: std::sync::Mutex<utoipa::openapi::OpenApi>,
}
pub type SharedBuildApiCtx = std::sync::Arc<BuildApiContextInner>;

/// Module trait to define startup dependencies
pub trait Module
where
    Self: Sized,
{
    type Context;

    fn build(
        bus: SharedMessageBus,
        ctx: Self::Context,
    ) -> impl futures::Future<Output = Result<Self>> + Send;
    fn run(&mut self) -> impl futures::Future<Output = Result<()>> + Send;

    fn load_from_disk<S>(file: &Path) -> Option<S>
    where
        S: borsh::BorshDeserialize,
    {
        match fs::File::open(file) {
            Ok(mut reader) => {
                info!("Loaded data from disk {}", file.to_string_lossy());
                log_error!(
                    borsh::from_reader(&mut reader),
                    "Loading and decoding {}",
                    file.to_string_lossy()
                )
                .ok()
            }
            Err(_) => {
                info!(
                    "File {} not found for module {} (using default)",
                    file.to_string_lossy(),
                    type_name::<S>(),
                );
                None
            }
        }
    }

    fn persist(&mut self) -> impl futures::Future<Output = Result<()>> + Send {
        async {
            info!(
                "Persistance is not implemented for module {}",
                type_name::<Self>()
            );
            Ok(())
        }
    }

    fn load_from_disk_or_default<S>(file: &Path) -> S
    where
        S: borsh::BorshDeserialize + Default,
    {
        Self::load_from_disk(file).unwrap_or(S::default())
    }

    fn save_on_disk<S>(file: &Path, store: &S) -> Result<()>
    where
        S: borsh::BorshSerialize,
    {
        // TODO/FIXME: Concurrent writes can happen, and an older state can override a newer one
        // Example:
        // State 1 starts creating a tmp file data.state1.tmp
        // State 2 starts creating a tmp file data.state2.tmp
        // rename data.state2.tmp into store (atomic override)
        // renemae data.state1.tmp into
        let salt: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect();
        let tmp = file.with_extension(format!("{}.tmp", salt));
        debug!("Saving on disk in a tmp file {:?}", tmp.clone());
        let mut buf_writer =
            BufWriter::new(log_error!(fs::File::create(tmp.as_path()), "Create file")?);
        log_error!(
            borsh::to_writer(&mut buf_writer, store),
            "Serializing Ctx chain"
        )?;

        log_error!(
            buf_writer.flush(),
            "Flushing Buffer writer for store {}",
            type_name::<S>()
        )?;
        debug!("Renaming {:?} to {:?}", &tmp, &file);
        log_error!(fs::rename(tmp, file), "Rename file")?;
        Ok(())
    }
}

struct ModuleStarter {
    pub name: &'static str,
    starter: Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'static>>,
}

pub mod signal {
    use std::any::TypeId;

    use crate::utils::static_type_map::Pick;

    #[derive(Clone, Debug)]
    pub struct PersistModule {}

    #[derive(Clone, Debug)]
    pub struct ShutdownModule {
        pub module: String,
    }

    #[derive(Clone, Debug)]
    pub struct ShutdownCompleted {
        pub module: String,
    }

    /// Execute a future, cancelling it if a shutdown signal is received.
    pub async fn shutdown_aware<M: 'static, F>(
        receiver: &mut impl Pick<
            tokio::sync::broadcast::Receiver<crate::modules::signal::ShutdownModule>,
        >,
        f: F,
    ) -> anyhow::Result<F::Output>
    where
        F: std::future::IntoFuture,
    {
        let mut dummy = false;
        tokio::select! {
            _ = async_receive_shutdown::<M>(
                &mut dummy,
                receiver.get_mut(),
            ) => {
                anyhow::bail!("Shutdown received");
            }
            res = f => {
                Ok(res)
            }
        }
    }

    /// Execute a future, cancelling it if a shutdown signal is received or a timeout is reached.
    pub async fn shutdown_aware_timeout<M: 'static, F>(
        receiver: &mut impl Pick<
            tokio::sync::broadcast::Receiver<crate::modules::signal::ShutdownModule>,
        >,
        duration: std::time::Duration,
        f: F,
    ) -> anyhow::Result<F::Output>
    where
        F: std::future::IntoFuture,
    {
        let mut dummy = false;
        tokio::select! {
            _ = tokio::time::sleep(duration) => {
                anyhow::bail!("Timeout reached");
            }
            _ = async_receive_shutdown::<M>(
                &mut dummy,
                receiver.get_mut(),
            ) => {
                anyhow::bail!("Shutdown received");
            }
            res = f => {
                Ok(res)
            }
        }
    }

    pub async fn async_receive_shutdown<T: 'static>(
        should_shutdown: &mut bool,
        shutdown_receiver: &mut tokio::sync::broadcast::Receiver<
            crate::modules::signal::ShutdownModule,
        >,
    ) -> anyhow::Result<()> {
        if *should_shutdown {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            return Ok(());
        }
        while let Ok(shutdown_event) = shutdown_receiver.recv().await {
            if TypeId::of::<T>() == TypeId::of::<()>() {
                tracing::debug!("Break signal received for any module");
                *should_shutdown = true;
                return Ok(());
            }
            if shutdown_event.module == std::any::type_name::<T>() {
                tracing::debug!(
                    "Break signal received for module {}",
                    std::any::type_name::<T>()
                );
                *should_shutdown = true;
                return Ok(());
            }
        }
        anyhow::bail!(
            "Error while shutting down module {}",
            std::any::type_name::<T>()
        );
    }
}

#[macro_export]
macro_rules! module_handle_messages {
    (on_self $self:expr, delay_shutdown_until  $lay_shutdow_until:block, $($rest:tt)*) => {
        {
            // Safety: this is disjoint.
            let mut shutdown_receiver = unsafe { &mut *$crate::utils::static_type_map::Pick::<tokio::sync::broadcast::Receiver<$crate::modules::signal::ShutdownModule>>::splitting_get_mut(&mut $self.bus) };
            let mut should_shutdown = false;
            $crate::handle_messages! {
                on_bus $self.bus,
                listen<$crate::modules::signal::PersistModule> _ => {
                    _ = $self.persist().await;
                }
                $($rest)*
                Ok(_) = $crate::modules::signal::async_receive_shutdown::<Self>(&mut should_shutdown, &mut shutdown_receiver) => {
                    let res = $lay_shutdow_until;
                    if res {
                        break;
                    }
                }
            }
            tracing::info!("Event loop listening to {} has stopped", module_path!());
            should_shutdown
        }

    };
    (on_self $self:expr, $($rest:tt)*) => {
        {
            // Safety: this is disjoint.
            let mut shutdown_receiver = unsafe { &mut *$crate::utils::static_type_map::Pick::<tokio::sync::broadcast::Receiver<$crate::modules::signal::ShutdownModule>>::splitting_get_mut(&mut $self.bus) };
            let mut should_shutdown = false;
            $crate::handle_messages! {
                on_bus $self.bus,
                listen<$crate::modules::signal::PersistModule> _ => {
                    _ = $self.persist().await;
                }
                $($rest)*
                Ok(_) = $crate::modules::signal::async_receive_shutdown::<Self>(&mut should_shutdown, &mut shutdown_receiver) => {
                    break;
                }
            }
            tracing::info!("Event loop listening in {} has stopped", module_path!());
            should_shutdown
        }
    };
}

#[macro_export]
macro_rules! module_bus_client {
    (
        $(#[$meta:meta])*
        $pub:vis struct $name:ident $(< $( $lt:tt $( : $clt:tt $(+ $dlt:tt )* )? ),+ >)? {
            $(sender($sender:ty),)*
            $(receiver($receiver:ty),)*
        }
    ) => {
        $crate::bus::bus_client!{
            $(#[$meta])*
            $pub struct $name $(< $( $lt $( : $clt $(+ $dlt )* )? ),+ >)? {
                $(sender($sender),)*
                $(receiver($receiver),)*
                receiver($crate::modules::signal::ShutdownModule),
                receiver($crate::modules::signal::PersistModule),
            }
        }
    }
}

pub use module_bus_client;

bus_client! {
    pub struct ShutdownClient {
        sender(signal::ShutdownModule),
        sender(signal::ShutdownCompleted),
        receiver(signal::ShutdownModule),
        receiver(signal::ShutdownCompleted),
    }
}

pub struct ModulesHandler {
    bus: SharedMessageBus,
    modules: Vec<ModuleStarter>,
    started_modules: Vec<&'static str>,
    running_modules: Vec<JoinHandle<()>>,
    shut_modules: Vec<String>,
}

impl ModulesHandler {
    pub async fn new(shared_bus: &SharedMessageBus) -> ModulesHandler {
        let shared_message_bus = shared_bus.new_handle();

        ModulesHandler {
            bus: shared_message_bus,
            modules: vec![],
            started_modules: vec![],
            running_modules: vec![],
            shut_modules: vec![],
        }
    }

    fn long_running_module(module_name: &str) -> bool {
        !["hyle::genesis::Genesis"].contains(&module_name)
    }

    pub async fn start_modules(&mut self) -> Result<()> {
        if !self.running_modules.is_empty() {
            bail!("Modules are already running!");
        }

        for module in self.modules.drain(..) {
            if Self::long_running_module(module.name) {
                self.started_modules.push(module.name);
            }

            debug!("Starting module {}", module.name);

            let mut shutdown_client = ShutdownClient::new_from_bus(self.bus.new_handle()).await;
            let mut shutdown_client2 = ShutdownClient::new_from_bus(self.bus.new_handle()).await;
            let task = tokio::spawn(async move {
                let module_task = tokio::spawn(module.starter);
                let timeout_task = tokio::spawn(async move {
                    loop {
                        if let Ok(signal::ShutdownModule { module: modname }) =
                            shutdown_client2.recv().await
                        {
                            if modname == module.name {
                                tokio::time::sleep(MODULE_SHUTDOWN_TIMEOUT).await;
                                break;
                            }
                        }
                    }
                });

                let res = tokio::select! {
                    res = module_task => {
                        res
                    },
                    _ = timeout_task => {
                        Ok(Err(anyhow::anyhow!("Shutdown timeout reached")))
                    }
                };
                match res {
                    Ok(Ok(())) => {
                        tracing::warn!(module =% module.name, "Module {} exited with no error.", module.name);
                    }
                    Ok(Err(e)) => {
                        tracing::error!(module =% module.name, "Module {} exited with error: {:?}", module.name, e);
                    }
                    Err(e) => {
                        tracing::error!(module =% module.name, "Module {} exited, error joining: {:?}", module.name, e);
                    }
                }

                _ = log_error!(
                    shutdown_client.send(signal::ShutdownCompleted {
                        module: module.name.to_string(),
                    }),
                    "Sending ShutdownCompleted message"
                );
            });

            if Self::long_running_module(module.name) {
                self.running_modules.push(task);
            }
        }

        Ok(())
    }

    /// Setup a loop waiting for shutdown signals from modules
    pub async fn shutdown_loop(&mut self) -> Result<()> {
        if self.started_modules.is_empty() {
            return Ok(());
        }

        let mut shutdown_client = ShutdownClient::new_from_bus(self.bus.new_handle()).await;

        // Trigger shutdown chain when one shutdown message is received for a long running module
        handle_messages! {
            on_bus shutdown_client,
            listen<signal::ShutdownCompleted> msg => {
                if Self::long_running_module(msg.module.as_str()) && !self.shut_modules.contains(&msg.module)  {
                    self.started_modules.retain(|module| *module != msg.module.clone());
                    self.shut_modules.push(msg.module.clone());
                    if self.started_modules.is_empty() {
                        break;
                    } else {
                        _ = self.shutdown_next_module().await;
                    }
                }
            }

            // Add one second as buffer to let the module cancel itself, hopefully.
            _ = tokio::time::sleep(MODULE_SHUTDOWN_TIMEOUT + Duration::from_secs(1)) => {
                if !self.shut_modules.is_empty() {
                    _ = self.shutdown_next_module().await;
                }
            }
        }

        Ok(())
    }

    /// Shutdown modules in reverse order (start A, B, C, shutdown C, B, A)
    async fn shutdown_next_module(&mut self) -> Result<()> {
        let mut shutdown_client = ShutdownClient::new_from_bus(self.bus.new_handle()).await;
        if let Some(module_name) = self.started_modules.pop() {
            // May be the shutdown message was skipped because the module failed somehow
            if !self.shut_modules.contains(&module_name.to_string()) {
                _ = log_error!(
                    shutdown_client.send(signal::ShutdownModule {
                        module: module_name.to_string(),
                    }),
                    "Shutting down module"
                );
            } else {
                tracing::debug!("Not shutting already shut module {}", module_name);
            }
        }

        Ok(())
    }

    pub async fn shutdown_modules(&mut self) -> Result<()> {
        self.shutdown_next_module().await?;
        self.shutdown_loop().await?;

        Ok(())
    }

    pub async fn exit_loop(&mut self) -> Result<()> {
        _ = log_error!(self.shutdown_loop().await, "Shutdown Loop triggered");
        _ = self.shutdown_modules().await;

        Ok(())
    }

    /// If the node is run as a process, we want to setup a proper exit loop with SIGINT/SIGTERM
    pub async fn exit_process(&mut self) -> Result<()> {
        #[cfg(unix)]
        {
            use tokio::signal::unix;
            let mut interrupt = unix::signal(unix::SignalKind::interrupt())?;
            let mut terminate = unix::signal(unix::SignalKind::terminate())?;
            tokio::select! {
                res = self.shutdown_loop() => {
                    _ = log_error!(res, "Shutdown Loop triggered");
                }
                _ = interrupt.recv() =>  {
                    info!("SIGINT received, shutting down");
                }
                _ = terminate.recv() =>  {
                    info!("SIGTERM received, shutting down");
                }
            }
            _ = self.shutdown_modules().await;
        }
        #[cfg(not(unix))]
        {
            tokio::select! {
                res = self.shutdown_loop() => {
                    _ = log_error!(res, "Shutdown Loop triggered");
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("Ctrl-C received, shutting down");
                }
            }
            _ = self.shutdown_modules().await;
        }
        Ok(())
    }

    async fn run_module<M>(mut module: M) -> Result<()>
    where
        M: Module,
    {
        module.run().await?;
        module.persist().await
    }

    pub async fn build_module<M>(&mut self, ctx: M::Context) -> Result<()>
    where
        M: Module + 'static + Send,
        <M as Module>::Context: std::marker::Send,
    {
        let module = M::build(self.bus.new_handle(), ctx).await?;
        self.add_module(module)
    }

    pub fn add_module<M>(&mut self, module: M) -> Result<()>
    where
        M: Module + 'static + Send,
        <M as Module>::Context: std::marker::Send,
    {
        debug!("Adding module {}", type_name::<M>());
        self.modules.push(ModuleStarter {
            name: type_name::<M>(),
            starter: Box::pin(Self::run_module(module)),
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::bus::{dont_use_this::get_receiver, metrics::BusMetrics};

    use super::*;
    use crate::bus::SharedMessageBus;
    use signal::{ShutdownCompleted, ShutdownModule};
    use std::{fs::File, sync::Arc};
    use tempfile::tempdir;
    use tokio::sync::Mutex;

    #[derive(Default, borsh::BorshSerialize, borsh::BorshDeserialize)]
    struct TestStruct {
        value: u32,
    }

    struct TestModule<T> {
        bus: TestBusClient,
        _field: T,
    }

    module_bus_client! {
        struct TestBusClient { sender(usize), }
    }

    macro_rules! test_module {
        ($bus_client:ty, $tag:ty) => {
            impl Module for TestModule<$tag> {
                type Context = ();
                async fn build(bus: SharedMessageBus, _ctx: Self::Context) -> Result<Self> {
                    Ok(TestModule {
                        bus: <$bus_client>::new_from_bus(bus).await,
                        _field: Default::default(),
                    })
                }

                async fn run(&mut self) -> Result<()> {
                    let nb_shutdowns: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
                    let cloned = Arc::clone(&nb_shutdowns);
                    module_handle_messages! {
                        on_self self,
                        _ = async {
                            let mut guard = cloned.lock().await;
                            (*guard) += 1;
                            std::future::pending::<()>().await
                        } => { }
                    };

                    self.bus.send(*cloned.lock().await).expect(
                        "Error while sending the number of loop cancellations while shutting down",
                    );

                    Ok(())
                }
            }
        };
    }

    test_module!(TestBusClient, String);
    test_module!(TestBusClient, usize);
    test_module!(TestBusClient, bool);

    // Failing module by breaking event loop

    impl Module for TestModule<u64> {
        type Context = ();
        async fn build(bus: SharedMessageBus, _ctx: Self::Context) -> Result<Self> {
            Ok(TestModule {
                bus: TestBusClient::new_from_bus(bus).await,
                _field: Default::default(),
            })
        }

        async fn run(&mut self) -> Result<()> {
            module_handle_messages! {
                on_self self,
                _ = async { } => {
                    break;
                }
            };

            Ok(())
        }
    }

    // Failing module by early exit (no shutdown completed event emitted)

    impl Module for TestModule<u32> {
        type Context = ();
        async fn build(bus: SharedMessageBus, _ctx: Self::Context) -> Result<Self> {
            Ok(TestModule {
                bus: TestBusClient::new_from_bus(bus).await,
                _field: Default::default(),
            })
        }

        async fn run(&mut self) -> Result<()> {
            panic!("bruh");
        }
    }

    #[test]
    fn test_load_from_disk_or_default() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_file");

        // Write a valid TestStruct to the file
        let mut file = File::create(&file_path).unwrap();
        let test_struct = TestStruct { value: 42 };
        borsh::to_writer(&mut file, &test_struct).unwrap();

        // Load the struct from the file
        let loaded_struct: TestStruct = TestModule::<usize>::load_from_disk_or_default(&file_path);
        assert_eq!(loaded_struct.value, 42);

        // Load from a non-existent file
        let non_existent_path = dir.path().join("non_existent_file");
        let default_struct: TestStruct =
            TestModule::<usize>::load_from_disk_or_default(&non_existent_path);
        assert_eq!(default_struct.value, 0);
    }

    #[test_log::test]
    fn test_save_on_disk() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_file.data");

        let test_struct = TestStruct { value: 42 };
        TestModule::<usize>::save_on_disk(&file_path, &test_struct).unwrap();

        // Load the struct from the file to verify it was saved correctly
        let loaded_struct: TestStruct = TestModule::<usize>::load_from_disk_or_default(&file_path);
        assert_eq!(loaded_struct.value, 42);
    }

    #[tokio::test]
    async fn test_build_module() {
        let shared_bus = SharedMessageBus::new(BusMetrics::global("id".to_string()));
        let mut handler = ModulesHandler::new(&shared_bus).await;
        handler.build_module::<TestModule<usize>>(()).await.unwrap();
        assert_eq!(handler.modules.len(), 1);
    }

    #[tokio::test]
    async fn test_add_module() {
        let shared_bus = SharedMessageBus::new(BusMetrics::global("id".to_string()));
        let mut handler = ModulesHandler::new(&shared_bus).await;
        let module = TestModule {
            bus: TestBusClient::new_from_bus(shared_bus.new_handle()).await,
            _field: 2_usize,
        };

        handler.add_module(module).unwrap();
        assert_eq!(handler.modules.len(), 1);
    }

    #[tokio::test]
    async fn test_start_modules() {
        let shared_bus = SharedMessageBus::new(BusMetrics::global("id".to_string()));
        let mut shutdown_receiver = get_receiver::<ShutdownModule>(&shared_bus).await;
        let mut shutdown_completed_receiver = get_receiver::<ShutdownCompleted>(&shared_bus).await;
        let mut handler = ModulesHandler::new(&shared_bus).await;
        handler.build_module::<TestModule<usize>>(()).await.unwrap();

        _ = handler.start_modules().await;
        _ = handler.shutdown_next_module().await;

        assert_eq!(
            shutdown_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<usize>>().to_string()
        );

        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<usize>>().to_string()
        );
    }

    // When modules are strated in the following order A, B, C, they should be closed in the reverse order C, B, A
    #[tokio::test]
    async fn test_start_stop_modules_in_order() {
        let shared_bus = SharedMessageBus::new(BusMetrics::global("id".to_string()));
        let mut shutdown_receiver = get_receiver::<ShutdownModule>(&shared_bus).await;
        let mut shutdown_completed_receiver = get_receiver::<ShutdownCompleted>(&shared_bus).await;
        let mut handler = ModulesHandler::new(&shared_bus).await;

        handler.build_module::<TestModule<usize>>(()).await.unwrap();
        handler
            .build_module::<TestModule<String>>(())
            .await
            .unwrap();
        _ = handler.start_modules().await;
        _ = handler.shutdown_modules().await;

        // Shutdown last module first
        assert_eq!(
            shutdown_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<String>>().to_string()
        );

        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<String>>().to_string()
        );

        // Then first module at last
        assert_eq!(
            shutdown_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<usize>>().to_string()
        );

        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<usize>>().to_string()
        );
    }

    #[tokio::test]
    async fn test_shutdown_modules_exactly_once() {
        let shared_bus = SharedMessageBus::new(BusMetrics::global("id".to_string()));
        let mut cancellation_counter_receiver = get_receiver::<usize>(&shared_bus).await;
        let mut shutdown_completed_receiver = get_receiver::<ShutdownCompleted>(&shared_bus).await;
        let mut handler = ModulesHandler::new(&shared_bus).await;

        handler.build_module::<TestModule<usize>>(()).await.unwrap();
        handler
            .build_module::<TestModule<String>>(())
            .await
            .unwrap();
        handler.build_module::<TestModule<bool>>(()).await.unwrap();

        _ = handler.start_modules().await;
        _ = tokio::time::sleep(Duration::from_millis(100)).await;
        _ = handler.shutdown_modules().await;

        // Shutdown last module first
        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<bool>>().to_string()
        );

        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<String>>().to_string()
        );

        // Then first module at last

        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<usize>>().to_string()
        );

        assert_eq!(cancellation_counter_receiver.try_recv().expect("1"), 1);
        assert_eq!(cancellation_counter_receiver.try_recv().expect("1"), 1);
        assert_eq!(cancellation_counter_receiver.try_recv().expect("1"), 1);
    }

    // in case a module fails, it will emit a shutdowncompleted event that will trigger the shutdown loop and shut all other modules
    // All other modules are shut in the right order
    #[tokio::test]
    async fn test_shutdown_all_modules_if_one_fails() {
        let shared_bus = SharedMessageBus::new(BusMetrics::global("id".to_string()));
        let mut shutdown_completed_receiver = get_receiver::<ShutdownCompleted>(&shared_bus).await;
        let mut handler = ModulesHandler::new(&shared_bus).await;

        handler.build_module::<TestModule<usize>>(()).await.unwrap();
        handler
            .build_module::<TestModule<String>>(())
            .await
            .unwrap();
        handler.build_module::<TestModule<u64>>(()).await.unwrap();
        handler.build_module::<TestModule<bool>>(()).await.unwrap();

        _ = handler.start_modules().await;

        // Starting shutdown loop should shut all modules because one failed immediatly

        _ = handler.shutdown_loop().await;

        // u64 module fails first, emits two events, one because it is the first task to end,
        // and the other because it finished to shutdown corretly
        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<u64>>().to_string()
        );

        // Shutdown last module first
        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<bool>>().to_string()
        );

        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<String>>().to_string()
        );

        // Then first module at last

        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<usize>>().to_string()
        );
    }

    // in case a module panics,
    // the module panic listener will know the task has ended, and will trigger a shutdown completed event
    // the other modules will shut in the right order
    #[tokio::test]
    async fn test_shutdown_all_modules_if_one_module_panics() {
        let shared_bus = SharedMessageBus::new(BusMetrics::global("id".to_string()));
        let mut shutdown_completed_receiver = get_receiver::<ShutdownCompleted>(&shared_bus).await;
        let mut handler = ModulesHandler::new(&shared_bus).await;

        handler.build_module::<TestModule<usize>>(()).await.unwrap();
        handler
            .build_module::<TestModule<String>>(())
            .await
            .unwrap();
        handler.build_module::<TestModule<u32>>(()).await.unwrap();
        handler.build_module::<TestModule<bool>>(()).await.unwrap();

        _ = handler.start_modules().await;

        // Starting shutdown loop should shut all modules because one failed immediatly

        _ = handler.shutdown_loop().await;

        // u32 module failed with panic, but the event should be emitted

        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<u32>>().to_string()
        );

        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<bool>>().to_string()
        );

        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<String>>().to_string()
        );

        assert_eq!(
            shutdown_completed_receiver.recv().await.unwrap().module,
            std::any::type_name::<TestModule<usize>>().to_string()
        );
    }
}
