# Dynamic Consistency Boundary Event Store - ESDB adapter

[EventSourcing Database](https://www.eventsourcingdb.io/) adapter for the [Dynamic Consistency Boundary implementation](https://github.com/bwaidelich/dcb-eventstore).

## Usage

### Installation

Install via [composer](https://getcomposer.org):

```shell
composer require wwwision/dcb-eventstore-esdb
```

### Create instance

```php
use Wwwision\DCBEventStoreESDB\ESDBEventStore;

$eventStore = ESDBEventStore::create(baseUri: 'localhost:3000', apiKey: 'secret');
```

### Specify custom event source / type prefix

EventSourcing Database expects each event to have a [source](https://docs.eventsourcingdb.io/fundamentals/sources/) specified.
Furthermore, [event types](https://docs.eventsourcingdb.io/fundamentals/event-types/) have to be namespaced.

By default, the `eventSource` is set to "https://dcb.events" and the event type is prefixed with "events.dcb." to be compliant.
Those values are removed when reading events via the `ESDBEventStore` client, so the values are not really important.
However, they can be changed via the `with()` function:

```php
// ...
$eventStore = $eventStore->with(
  eventSource: 'https://domain.tld',
  eventTypePrefix: 'tld.domain.'
);
```

See [wwwision/dcb-eventstore](https://github.com/bwaidelich/dcb-eventstore) for more details and usage examples

## Contribution

Contributions in the form of [issues](https://github.com/bwaidelich/dcb-eventstore-esdb/issues) or [pull requests](https://github.com/bwaidelich/dcb-eventstore-esdb/pulls) are highly appreciated

## License

See [LICENSE](./LICENSE)