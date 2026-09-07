wit_bindgen::generate!({
    path: "../../../wit",
    world: "cell",
    generate_all,
});

use std::cell::RefCell;

use serde::Serialize;

use exports::wonderful::cells::guest::{CommandOutput, Effect, Guest, GuestError};

thread_local! {
    static STATE: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

#[derive(Serialize)]
struct ScheduleAtEffect {
    schedule_id: String,
    timestamp_ms: i64,
    method: String,
    input: Vec<u8>,
}

#[derive(Serialize)]
struct SocketSendEffect {
    connection_id: String,
    payload: Vec<u8>,
}

struct TestCell;

impl Guest for TestCell {
    fn activate(
        _bundle: Vec<u8>,
        _manifest: Vec<u8>,
        initial_state: Vec<u8>,
        _input: Option<Vec<u8>>,
    ) -> Result<(), GuestError> {
        STATE.with_borrow_mut(|state| *state = initial_state);
        Ok(())
    }

    fn invoke_command(name: String, input: Vec<u8>) -> Result<CommandOutput, GuestError> {
        if name == "__wonderful_socket_connect" || name == "__wonderful_socket_close" {
            let state = STATE.with_borrow(Clone::clone);
            return Ok(CommandOutput {
                state,
                value: Vec::new(),
                effects: Vec::new(),
                destroyed: false,
            });
        }
        if name == "spin" {
            loop {
                STATE.with_borrow_mut(|state| {
                    state.push(1);
                    state.pop();
                });
            }
        }
        if name == "read" || name == "read_sql" || name == "mutating_query" {
            let value = if name == "read_sql" {
                let result =
                    wonderful::cells::host::execute_sql("SELECT value FROM test_values", &[])
                        .map_err(|message| GuestError {
                            code: "TEST_SQL_FAILURE".to_owned(),
                            message,
                        })?;
                vec![u8::try_from(result.rows.len()).unwrap_or(u8::MAX)]
            } else if name == "mutating_query" {
                wonderful::cells::host::execute_sql(
                    "INSERT INTO test_values (value) VALUES (?)",
                    &[wonderful::cells::host::SqlValue::Blob(input.clone())],
                )
                .map_err(|message| GuestError {
                    code: "TEST_SQL_FAILURE".to_owned(),
                    message,
                })?;
                Vec::new()
            } else {
                STATE.with_borrow(Clone::clone)
            };
            return Ok(CommandOutput {
                state: STATE.with_borrow(Clone::clone),
                value,
                effects: Vec::new(),
                destroyed: false,
            });
        }
        if name == "destroy" {
            let state = STATE.with_borrow(Clone::clone);
            return Ok(CommandOutput {
                state,
                value: input,
                effects: Vec::new(),
                destroyed: true,
            });
        }
        if name == "fail" {
            STATE.with_borrow_mut(|state| state.extend_from_slice(&input));
            return Err(GuestError {
                code: "TEST_FAILURE".to_owned(),
                message: "requested failure".to_owned(),
            });
        }
        if name == "sql" {
            wonderful::cells::host::execute_sql(
                "CREATE TABLE IF NOT EXISTS test_values (value BLOB NOT NULL)",
                &[],
            )
            .map_err(|message| GuestError {
                code: "TEST_SQL_FAILURE".to_owned(),
                message,
            })?;
            let result = wonderful::cells::host::execute_sql(
                "INSERT INTO test_values (value) VALUES (?)",
                &[wonderful::cells::host::SqlValue::Blob(input.clone())],
            )
            .map_err(|message| GuestError {
                code: "TEST_SQL_FAILURE".to_owned(),
                message,
            })?;
            if result.changes != 1 {
                return Err(GuestError {
                    code: "TEST_SQL_CHANGES".to_owned(),
                    message: "unexpected SQL change count".to_owned(),
                });
            }
        }
        let state = if name == "send" {
            STATE.with_borrow(Clone::clone)
        } else {
            STATE.with_borrow_mut(|state| {
                state.extend_from_slice(&input);
                state.clone()
            })
        };
        let effects = if name == "schedule" {
            let mut payload = 1_u16.to_le_bytes().to_vec();
            payload.extend(
                serde_bare::to_vec(&ScheduleAtEffect {
                    schedule_id: "test-alarm".to_owned(),
                    timestamp_ms: 10_000,
                    method: "echo".to_owned(),
                    input: input.clone(),
                })
                .expect("encode schedule effect"),
            );
            vec![Effect {
                kind: "wonderful.cell.schedule.at".to_owned(),
                payload,
            }]
        } else if name == "send" {
            let connection_id = String::from_utf8(input.clone()).expect("connection ID input");
            [vec![1], vec![2]]
                .into_iter()
                .map(|message| {
                    let mut payload = 1_u16.to_le_bytes().to_vec();
                    payload.extend(
                        serde_bare::to_vec(&SocketSendEffect {
                            connection_id: connection_id.clone(),
                            payload: message,
                        })
                        .expect("encode socket send effect"),
                    );
                    Effect {
                        kind: "wonderful.cell.socket.send".to_owned(),
                        payload,
                    }
                })
                .collect()
        } else {
            Vec::new()
        };
        Ok(CommandOutput {
            state,
            value: input,
            effects,
            destroyed: false,
        })
    }

    fn snapshot() -> Vec<u8> {
        STATE.with_borrow(Clone::clone)
    }
}

export!(TestCell);
