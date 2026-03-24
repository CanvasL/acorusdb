use crate::error::{AcorusError, AcorusResult};

#[derive(Debug, Clone, Copy)]
pub enum ShutdownSignal {
    CtrlC,
    Sigterm,
}

impl ShutdownSignal {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CtrlC => "ctrl_c",
            Self::Sigterm => "sigterm",
        }
    }
}

pub async fn wait_for_shutdown_signal() -> AcorusResult<ShutdownSignal> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sigterm = signal(SignalKind::terminate()).map_err(AcorusError::ShutdownSignal)?;

        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result.map_err(AcorusError::ShutdownSignal)?;
                Ok(ShutdownSignal::CtrlC)
            }
            _ = sigterm.recv() => Ok(ShutdownSignal::Sigterm),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .map_err(AcorusError::ShutdownSignal)?;
        Ok(ShutdownSignal::CtrlC)
    }
}
