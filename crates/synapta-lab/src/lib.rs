//! Synapta Lab: many real Synapta nodes in one process on a simulated network of rings, driven
//! from a web page. See `docs/web-sandbox-plan.md`.
//!
//! | module     | job                                                        |
//! |------------|------------------------------------------------------------|
//! | `lab`      | the simulation: create, power, rewire and remove nodes     |
//! | `recorder` | recent trace events from every node, for live view/replay  |

pub mod lab;
pub mod recorder;

pub use lab::{CapabilitySetup, Lab, NodeSetup, NodeView, PowerOff};
