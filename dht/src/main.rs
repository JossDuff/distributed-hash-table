use anyhow::Result;
use clap::Parser;
use dht::{Config, Node};
use tracing::error;

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()?;

    let net_handle = rt.handle().clone();

    rt.block_on(async {
        let config = Config::parse();
        info_node(&config);
        let node = Node::<u64, u8>::new(config, &net_handle).await?;
        if let Err(e) = node.run().await {
            error!("{e}");
        }
        Ok(())
    })
}

fn info_node(config: &Config) {
    tracing::info!("Node {} starting", config.name);
    tracing::info!("Connections: {:?}", config.connections);
}
