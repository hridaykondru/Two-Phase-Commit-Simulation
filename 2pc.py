import threading
import time
import random
from queue import Queue

class Coordinator(threading.Thread):
    def __init__(self, participants, participant_queues):
        threading.Thread.__init__(self)
        self.participants = participants
        self.participant_queues = participant_queues
        self.data = "Initial Data"
        self.transaction = "New Data"
    def send_query_to_commit(self):
        # Simulate sending query to commit to all participants
        print(f"Coordinator: Sending query to update '{self.data}' to '{self.transaction}' to all participants\n")
        for participant in self.participants:
            self.participant_queues[participant].put(("query", self.transaction))

    def receive_votes(self):
        # Simulate receiving votes from participants
        votes = {}
        for participant in self.participants:
            _,vote = self.participant_queues[participant].get()
            votes[participant] = vote
        print(f"Coordinator: Received votes from participants: {votes}\n")
        return votes

    def send_commit_message(self):
        # Simulate sending commit message to all participants
        print("Coordinator: Sending commit message to all participants\n")
        for participant in self.participants:
            self.participant_queues[participant].put(("commit", None))

    def send_rollback_message(self):
        # Sending rollback message to all participants
        print("Coordinator: Sending rollback message to all participants\n")
        for participant in self.participants:
            self.participant_queues[participant].put(("rollback", None))

    def receive_acknowledgements(self):
        # Receiving acknowledgements from participants
        print("Coordinator: Waiting for acknowledgements from participants\n")
        time.sleep(2)  # Simulate a delay
        acks = {}
        for participant in self.participants:
            ack, status = self.participant_queues[participant].get()
            acks[participant] = (ack, status)
        print(f"Coordinator: Received acknowledgements from participants: {acks}\n")
        # Check if all the participants have committed and then commit
        if all([status == "commit" for _, status in acks.values()]):
            self.data = self.transaction
            print(f"Coordinator: Transaction committed '{self.data}' successfully\n")
        else:
            print(f"Coordinator: Transaction aborted and record remains as '{self.data}'\n")

    def run(self):
        self.send_query_to_commit()
        time.sleep(2)  # Simulate a delay
        votes = self.receive_votes()

        if "abort" in votes.values():
            self.send_rollback_message()
        else:
            self.send_commit_message()

        self.receive_acknowledgements()

class Participant(threading.Thread):
    def __init__(self, name, participant_queue):
        super().__init__()
        self.name = name
        self.data="Initial Data"
        self.participant_queue = participant_queue
        self.log = []

    def vote_to_commit(self):
        # Simulate participant voting to commit or abort
        choice = random.choices(["commit", "abort"], weights=[0.85, 0.15], k=1)[0]
        print(f"{self.name}: Voting to {choice}\n")
        return choice

    def receive_query_message(self):
        # Simulate receiving query message
        _,new_data=self.participant_queue.get()
        print(f"{self.name}: Received query message to update '{self.data}' to '{new_data}'\n")
        self.log.append(self.data)
        self.data=new_data
        self.participant_queue.put(("vote", self.vote_to_commit()))

    def receive_commit_message(self):
        # Simulate receiving commit message
        print(f"{self.name}: Received commit message. Committed '{self.data}'\n")

    def receive_rollback_message(self):
        # Simulate receiving rollback message
        old_data=self.log.pop()
        print(f"{self.name}: Received rollback message. Rolling back '{self.data}' to '{old_data}'\n")
        self.data=old_data

    def send_acknowledgement(self):
        # Simulate sending acknowledgement to coordinator
        print(f"{self.name}: Sending acknowledgement to coordinator\n")
        self.participant_queue.put(("ack", self.commit_status))

    def run(self):
        self.receive_query_message()
        time.sleep(2)  # Simulate a delay
        self.commit_status, _ = self.participant_queue.get()
        if self.commit_status == "rollback":
            self.receive_rollback_message()
        else:
            self.receive_commit_message()

        self.send_acknowledgement()

# Simulation
participant_names = [f"Participant{i}" for i in range(1, 4)]
participant_queues = {f"Participant{i}": Queue() for i in range(1, 4)}
participants = [
    Participant(f"Participant{i}", participant_queues[f"Participant{i}"])
    for i in range(1, 4)
]
coordinator = Coordinator(participant_names, participant_queues)

threads = [coordinator] + participants

for thread in threads:
    thread.start()

for thread in threads:
    thread.join()

print("Simulation complete")
