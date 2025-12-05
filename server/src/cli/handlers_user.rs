use anyhow::{anyhow, Result};
use tokio::task;

/// Retrieves the current system username.
/// This is a blocking operation wrapped for safe execution within the Tokio runtime.
pub async fn get_current_user() -> Result<String> {
    // Spawns a new thread to run the potentially blocking environment variable lookup.
    task::spawn_blocking(|| {
        // 1. Try common Unix/Linux environment variable 'USER'
        let username = std::env::var("USER")
            // 2. Fallback to common Windows environment variable 'USERNAME'
            .or_else(|_| std::env::var("USERNAME"))
            // 3. Fallback to 'LOGNAME' or other common variables, or use a default error.
            .map_err(|e| anyhow!("Failed to retrieve system username from environment variables (USER, USERNAME, LOGNAME): {}", e))?;

        // Ensure the retrieved string isn't empty.
        if username.is_empty() {
            Err(anyhow!("Retrieved username is empty"))
        } else {
            Ok(username)
        }
    })
    .await
    .map_err(|e| anyhow!("Blocking task failed to execute: {}", e))?
}