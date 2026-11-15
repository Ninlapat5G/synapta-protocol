//! Synapta Lab: many real Synapta nodes in one process on a simulated network of rings, driven
//! from a web page. See `docs/web-sandbox-plan.md`.
//!
//! | module     | job                                                        |
//! |------------|------------------------------------------------------------|
//! | `lab`      | the simulation: create, power, rewire and remove nodes     |
//! | `recorder` | recent trace events from every node, for live view/replay  |
//! | `scenarios`| one-click presets: build rings, bursts, crashes, cut bridges |
//! | `api`      | HTTP routes: the page's files and the JSON API over `Lab`  |
//! | `http`     | the small HTTP server those routes run on                  |
//! | `listen`   | which addresses the web page may listen on                 |

pub mod api;
pub mod http;
pub mod lab;
pub mod listen;
pub mod recorder;
pub mod scenarios;

pub use lab::{CapabilitySetup, Lab, NodeSetup, NodeView, PowerOff};
