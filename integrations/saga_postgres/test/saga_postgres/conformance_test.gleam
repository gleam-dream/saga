//// The PostgreSQL storage passes saga's storage conformance suite
//// (`saga/storage/conformance`), each scenario in a schema of its own.

import gleeunit/should
import saga/storage/conformance
import saga_postgres
import saga_postgres/support

const lease = 1000

pub fn the_postgres_storage_conforms_test() {
  use connection <- support.using_pool(10)
  conformance.run(
    fn() {
      let schema = support.schema()
      let config = support.migrated(connection, schema, lease)
      Ok(
        conformance.fixture(saga_postgres.storage(config), cleanup: fn() {
          support.drop(connection, schema)
        }),
      )
    },
    timeout: 5000,
    // A lost owner's claim ends when its lease expires.
    owner_loss_within: lease + 500,
  )
  |> should.equal(Ok(Nil))
}
