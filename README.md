# umsg-port : Simple Message Port

A Rust library to send and receive messages through AMQP (using Lapin) with ease.

## Features

- Easy to use API for sending and receiving messages in json form.
- Supports multiple message types: `Update`, `State`, `Task`, and `Result`.
- Configurable options for connection, heartbeat, and message expiration.


## Example

Check the `examples` directory for a dummy client and worker that demonstrate how to use the library.

To run the example:

```sh
cargo run --example dummy
```

## License

This library is open source and licensed under the MIT License.
