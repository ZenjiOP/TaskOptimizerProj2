import csv
from collections import defaultdict, deque
import tkinter as tk
from tkinter import ttk


class TaskNode:
    def __init__(self, task_id, dependencies, duration):
        self.task_id = task_id
        self.dependencies = dependencies
        self.duration = duration
        self.start_time = None
        self.end_time = None


class TaskListDAG:
    def __init__(self):
        self.graph = defaultdict(list)
        self.tasks = {}

    def add_task(self, task_node):
        task_id = task_node.task_id
        if task_id in self.tasks:
            raise ValueError(f"Task '{task_id}' already exists in the DAG")

        self.tasks[task_id] = task_node

        # Add edges to represent dependencies
        for dependency in task_node.dependencies:
            if dependency not in self.tasks:
                self.add_task(TaskNode(dependency, [], 1))  # Add dependency as a placeholder task
            self.graph[dependency].append(task_id)

        if task_id not in self.graph:
            self.graph[task_id] = []

    def topological_sort_by_levels(self):
        in_degree = {task: 0 for task in self.tasks}
        for task in self.graph:
            for neighbor in self.graph[task]:
                in_degree[neighbor] += 1

        queue = deque(sorted([task for task in self.tasks if in_degree[task] == 0]))
        levels = []

        while queue:
            level = []
            for _ in range(len(queue)):
                current_task = queue.popleft()
                level.append(current_task)

                for neighbor in sorted(self.graph[current_task]):
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)

            levels.append(level)

        if sum(len(level) for level in levels) != len(self.tasks):
            raise ValueError("Cycle detected in the DAG")

        return levels


def assign_workers(task_list, max_days=101):
    levels = task_list.topological_sort_by_levels()
    workers = []

    # Store worker end times
    worker_end_times = []

    for level in levels:
        for task_id in level:
            task = task_list.tasks[task_id]
            assigned = False

            # Try to assign task to an existing worker
            for i, end_time in enumerate(worker_end_times):
                if end_time + task.duration <= max_days:
                    workers[i].append(task)
                    worker_end_times[i] += task.duration
                    assigned = True
                    break

            # Add a new worker if no existing worker can take the task
            if not assigned:
                if task.duration <= max_days:  # Ensure the task fits within the max days limit
                    workers.append([task])
                    worker_end_times.append(task.duration)
                else:
                    raise ValueError(f"Task {task.task_id} exceeds the maximum allowed duration")

    return workers


# Function to load and process tasks
def load_and_process_tasks(file_name):
    task_list = TaskListDAG()

    with open(file_name, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            task_id = row["Main Task"]
            dependencies = [dep.strip() for dep in row["Dependent Task"].split(',')] if row["Dependent Task"] else []
            duration = int(row["Time for Main Task (days)"])
            task_list.add_task(TaskNode(task_id, dependencies, duration))

    workers = assign_workers(task_list, max_days=101)
    return workers


# Function to create the UI
def create_ui(workers):
    root = tk.Tk()
    root.title("Job Scheduling Results")

    # Create a treeview to display the worker results
    tree = ttk.Treeview(root, columns=("Tasks", "Durations"), show="headings")
    tree.heading("Tasks", text="Tasks Assigned")
    tree.heading("Durations", text="Durations (days)")

    # Add data to the treeview
    for i, worker in enumerate(workers, 1):
        tasks = ", ".join(task.task_id for task in worker)
        durations = ", ".join(str(task.duration) for task in worker)
        tree.insert("", "end", values=(tasks, durations))

    # Add the treeview to the UI
    tree.pack(fill="both", expand=True)

    # Run the UI loop
    root.mainloop()


# Main script
if __name__ == "__main__":
    file_name = "Task_Dependencies_and_Time_Dataframe_Revised.csv"
    workers = load_and_process_tasks(file_name)
    print(f"Minimum number of workers required: {len(workers)}")
    for i, worker in enumerate(workers, 1):
        tasks = [task.task_id for task in worker]
        durations = [task.duration for task in worker]
        print(f"Worker {i}: Tasks {tasks}, Durations {durations}")

    # Launch the UI
    create_ui(workers)

