import {
  catchError,
  concatMap,
  delay,
  EMPTY,
  finalize,
  from,
  groupBy,
  map,
  mergeMap,
  of,
  Subject,
  tap,
  throwError,
} from "rxjs";

enum MachineState {
  CYCLING = "CYCLING",
  IDLE = "IDLE",
  ALARM = "ALARM",
  DOWN = "DOWN",
}

interface MachineStateEvent {
  machineId: string;
  state: MachineState;
  subState?: string;
  timestamp: number;
}

export interface MachineStateWithDuration {
  state: MachineState;
  substate?: string;
  duration: number;
}

const machineStateWithDuration: MachineStateWithDuration[] = [
  {
    state: MachineState.CYCLING,
    substate: "Chargement",
    duration: 5000,
  },
  {
    state: MachineState.CYCLING,
    substate: "Production",
    duration: 5000,
  },
  {
    state: MachineState.ALARM,
    substate: "Casse outil",
    duration: 5000,
  },
  {
    state: MachineState.IDLE,
    duration: 5000,
  },
];

const NUMBER_OF_MACHINES = 300;
const PROCESSING_DURATION = 10000; // Processing duration for each event
const FAILURE_PROBABILITY = 0.1; // 10% chance to fail

const createMachineStateEvent = (
  machineId: string,
  stateWithDuration: MachineStateWithDuration,
): MachineStateEvent => {
  return {
    machineId,
    state: stateWithDuration.state,
    subState: stateWithDuration.substate,
    timestamp: Date.now(),
  };
};

const machineStatesSubject = new Subject<MachineStateEvent>();

// Simulate an asynchronous task with a delay
const simulateProcessing = (event: MachineStateEvent): Promise<void> => {
  return new Promise((resolve, reject) => {
    console.log(
      `Starting processing event for machine "${event.machineId}"...`,
      event,
    );

    // Simulate random error
    const shouldFail = Math.random() < FAILURE_PROBABILITY; // 10% chance to fail

    setTimeout(() => {
      if (shouldFail) {
        reject(`Processing failed for machine "${event.machineId}"`);
      } else {
        resolve();
      }
    }, PROCESSING_DURATION);
  });
};

// Group events by machineId and process them in parallel
machineStatesSubject
  .pipe(
    groupBy((event) => event.machineId),
    mergeMap((group$) =>
      group$.pipe(
        concatMap((event) =>
          from(simulateProcessing(event)).pipe(
            tap(() => {
              console.log(
                `Event for machine "${event.machineId}" processed successfully`,
                event,
              );
            }),
            catchError((error) => {
              console.error("Error processing events:", error);
              return EMPTY;
            }),
          ),
        ),
      ),
    ),
    finalize(() => {
      console.log("All events have been processed.");
    }),
  )
  .subscribe();

// Send all machine state events
let eventsSent = 0;
const totalEvents = NUMBER_OF_MACHINES * machineStateWithDuration.length;

for (let i = 0; i < NUMBER_OF_MACHINES; i++) {
  const machineId = `machine-${i}`;

  const sendMachineStateEvent = (machineId: string, eventIndex: number) => {
    const eventWithDuration = machineStateWithDuration[eventIndex];
    const event = createMachineStateEvent(machineId, eventWithDuration);
    machineStatesSubject.next(event);
    eventsSent++;

    if (eventIndex >= machineStateWithDuration.length - 1) {
      if (eventsSent >= totalEvents) {
        machineStatesSubject.complete(); // Complete the subject when all events have been sent
      }
      return;
    }

    setTimeout(() => {
      sendMachineStateEvent(machineId, eventIndex + 1);
    }, eventWithDuration.duration);
  };

  sendMachineStateEvent(machineId, 0);
}
