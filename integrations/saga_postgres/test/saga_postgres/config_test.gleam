//// The configuration's defaults and bounds, and what `storage` declares
//// to saga.

import gleam/option.{Some}
import gleam/string
import gleeunit/should
import pog
import saga/storage
import saga_postgres
import saga_postgres/support

fn renewal_every(config: saga_postgres.Config) -> Int {
  let assert Some(#(every, _)) = storage.renewal(saga_postgres.storage(config))
  every
}

pub fn storage_renews_every_third_of_the_lease_test() {
  use connection <- support.using_pool(1)
  let config = saga_postgres.config(connection)
  renewal_every(config) |> should.equal(10_000)
  renewal_every(config |> saga_postgres.with_lease(3000))
  |> should.equal(1000)
}

pub fn a_lease_below_100_ms_is_raised_to_100_test() {
  use connection <- support.using_pool(1)
  renewal_every(saga_postgres.config(connection) |> saga_postgres.with_lease(5))
  |> should.equal(33)
}

pub fn storage_keeps_saga_default_call_timeout_test() {
  use connection <- support.using_pool(1)
  storage.call_timeout(saga_postgres.storage(saga_postgres.config(connection)))
  |> should.equal(5000)
}

const secret = "saga-postgres-inspect-secret-7c41"

/// The configuration keeps only the application's `pog.Connection`, which
/// names its pool, so inspecting it prints no credential.
pub fn inspecting_a_config_never_prints_the_password_test() {
  // The throwaway cluster trusts every connection, so it accepts any
  // password; the pool still holds this one in its configuration.
  use connection <- support.using_pool_with(1, fn(config) {
    pog.password(config, Some(secret))
  })
  let config = support.migrated(connection, support.schema(), 30_000)
  string.contains(string.inspect(config), secret) |> should.be_false
  string.contains(string.inspect(saga_postgres.storage(config)), secret)
  |> should.be_false
}

pub fn inspecting_a_migration_failure_never_prints_the_password_test() {
  use connection <- support.using_pool_with(1, fn(config) {
    config |> pog.database("nowhere") |> pog.password(Some(secret))
  })
  let assert Error(failure) =
    saga_postgres.migrate(saga_postgres.config(connection))
  string.contains(string.inspect(failure), secret) |> should.be_false
  string.contains(saga_postgres.describe_migrate_error(failure), secret)
  |> should.be_false
}
