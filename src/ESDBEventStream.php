<?php

declare(strict_types=1);

namespace Wwwision\DCBEventStoreESDB;

use DateTimeImmutable;
use Traversable;
use Webmozart\Assert\Assert;
use Wwwision\DCBEventStore\EventStream;
use Wwwision\DCBEventStore\Types\Event;
use Wwwision\DCBEventStore\Types\EventEnvelope;
use Wwwision\DCBEventStore\Types\SequenceNumber;

final readonly class ESDBEventStream implements EventStream
{
    /**
     * @param iterable<array<string, mixed>> $esdbEvents
     */
    public function __construct(
        private iterable $esdbEvents,
    ) {
    }

    public function getIterator(): Traversable
    {
        foreach ($this->esdbEvents as $esdbEvent) {
            yield $this->convertEvent($esdbEvent);
        }
    }

    public function first(): ?EventEnvelope
    {
        foreach ($this->esdbEvents as $event) {
            return $this->convertEvent($event);
        }
        return null;
    }

    // -----------------------------------

    /**
     * @param array<mixed> $event
     */
    private function convertEvent(array $event): EventEnvelope
    {
        Assert::numeric($event['position']);
        $recordedAt = DateTimeImmutable::createFromFormat('Y-m-d H:i:s', $event['recorded_at']);
        Assert::isInstanceOf($recordedAt, DateTimeImmutable::class);
        return new EventEnvelope(
            SequenceNumber::fromInteger((int)$event['position']),
            $recordedAt,
            Event::create(
                $event['type'],
                $event['data'],
                $event['tags'],
                [],
            ),
        );
    }
}
