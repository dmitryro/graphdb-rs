// lib/src/handlers/handlers_medical.rs

use anyhow::{Result, Context, anyhow};
use log::{info, error};
use std::time::{SystemTime, UNIX_EPOCH};
use chrono::{DateTime, Utc, Duration as ChronoDuration};

use lib::commands::{
    ScheduleCommand,
    TimingCommand,
    ResusCommand,
    EmergCommand,
};

pub async fn handle_schedule_command(command: ScheduleCommand) -> Result<()> {
    println!("==> Schedule command executed...");
    Ok(())
}

pub async fn handle_schedule_command_interactive(command: ScheduleCommand) -> Result<()> {
    handle_schedule_command(command).await;
    Ok(())
}
 
pub async fn handle_resus_command(command: ResusCommand) -> Result<()> {
    println!("==> Resus command executed...");
    Ok(())
}

pub async fn handle_resus_command_interactive(command: ResusCommand) -> Result<()> {
    handle_resus_command(command).await;
    Ok(())
}
 
pub async fn handle_timing_command(command: TimingCommand) -> Result<()> {
    println!("==> Timing command executed...");
    Ok(())
}

pub async fn handle_timing_command_interactive(command: TimingCommand) -> Result<()> {
    handle_timing_command(command).await;
    Ok(())
}
 
pub async fn handle_emerg_command(command: EmergCommand) -> Result<()> {
    println!("==> Timing command executed...");
    Ok(())
}

pub async fn handle_emerg_command_interactive(command: EmergCommand) -> Result<()> {
    handle_emerg_command(command).await;
    Ok(())
}
 