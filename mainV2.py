from collections import defaultdict, deque

class TaskNode:
    def __init__(self, task_id, duration, dependencies=None):
        self.task_id = task_id
        self.duration = duration
        self.dependencies = dependencies if dependencies else []
        self.start_time = 0
        self.end_time = 0


class TaskListDAG:
    def __init__(self):
        self.graph = defaultdict(list)  # Adjacency list for dependencies
        self.tasks = {}  # Task details

    def add_task(self, task_node):
        self.tasks[task_node.task_id] = task_node
        for dependency in task_node.dependencies:
            self.graph[dependency].append(task_node.task_id)  # Added dependency handling logic (TH)

    def topological_sort_by_levels(self):
        in_degree = {task_id: 0 for task_id in self.tasks}  # Calculate in-degree for all tasks (TH)
        for task_id in self.graph:
            for neighbor in self.graph[task_id]:
                in_degree[neighbor] += 1  # Update in-degree for neighbors (TH)

        zero_in_degree = deque([task_id for task_id in self.tasks if in_degree[task_id] == 0])  # Initialize zero in-degree queue (TH)
        levels = []
        while zero_in_degree:
            level = []
            for _ in range(len(zero_in_degree)):
                task_id = zero_in_degree.popleft()
                level.append(task_id)
                for neighbor in self.graph[task_id]:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        zero_in_degree.append(neighbor)
            levels.append(level)  # Append the level to result (TH)
        return levels


def find_min_duration(tasks):
    levels = tasks.topological_sort_by_levels()  # Retrieve task levels (TH)
    min_duration = 0

    for level in levels:
        for task_id in level:
            task_node = tasks.tasks[task_id]  # Access TaskNode (TH)
            if task_node.dependencies:
                task_node.start_time = max(tasks.tasks[dep].end_time for dep in task_node.dependencies)  # Calculate start time (TH)
            else:
                task_node.start_time = 0
            task_node.end_time = task_node.start_time + task_node.duration  # Set end time (TH)
        min_duration = max(min_duration, max(task.end_time for task in map(tasks.tasks.get, level)))  # Update min duration (TH)

    return min_duration


def assign_workers(task_list):
    levels = task_list.topological_sort_by_levels()  # Retrieve levels from DAG (TH)
    workers = []

    minimum_project_duration = find_min_duration(task_list)  # Find minimum project duration (TH)

    for level, task_ids in enumerate(levels):
        if level == 0:
            for task_id in task_ids:
                worker = [task_list.tasks[task_id]]  # Assign initial tasks to workers (TH)
                workers.append(worker)
        else:
            for task_id in task_ids:
                task = task_list.tasks[task_id]
                assigned = False
                for worker in workers:
                    if all(dep not in [t.task_id for t in worker] for dep in task.dependencies):  # Check dependencies (TH)
                        worker.append(task)
                        assigned = True
                        break
                if not assigned:
                    workers.append([task])  # Assign task to a new worker (TH)

    workers = merge_workers(workers, minimum_project_duration)  # Merge workers based on project duration (TH)

    for i, worker in enumerate(workers, 1):
        task_ids = [task.task_id for task in worker]
        durations = [task.duration for task in worker]
        print(f"Worker {i}: Tasks {task_ids}, Durations {durations}")  # Display worker assignments (TH)

    return workers


def merge_workers(workers, max_duration):
    final_workers = []  # Final merged worker list (TH)
    current_merged_worker = []  # Temporary list to accumulate tasks (TH)
    current_duration = 0  # Track accumulated duration (TH)

    for worker in workers:
        if any(task.dependencies for task in worker):  # Skip if tasks have dependencies (TH)
            final_workers.append(worker)
            continue
        for task in worker:
            if current_duration + task.duration <= max_duration:  # Check if task fits within max duration (TH)
                current_merged_worker.append(task)
                current_duration += task.duration
            else:
                if current_merged_worker:
                    final_workers.append(current_merged_worker)  # Add accumulated tasks to final workers (TH)
                current_merged_worker = [task]  # Start a new merged worker (TH)
                current_duration = task.duration
    if current_merged_worker:
        final_workers.append(current_merged_worker)  # Append remaining tasks (TH)

    return final_workers


def main():
    task_list = TaskListDAG()
    task_list.add_task(TaskNode(1, 3))  # Task 1 with duration 3 (TH)
    task_list.add_task(TaskNode(2, 2, [1]))  # Task 2 depends on Task 1 (TH)
    task_list.add_task(TaskNode(3, 4, [1]))  # Task 3 depends on Task 1 (TH)
    task_list.add_task(TaskNode(4, 1, [2, 3]))  # Task 4 depends on Tasks 2 and 3 (TH)
    task_list.add_task(TaskNode(5, 2, [3]))  # Task 5 depends on Task 3 (TH)
    task_list.add_task(TaskNode(6, 1, [4, 5]))  # Task 6 depends on Tasks 4 and 5 (TH)

    print("Minimum project duration:", find_min_duration(task_list))  # Calculate and print project duration (TH)
    workers = assign_workers(task_list)
    print("Final worker assignments:")
    for i, worker in enumerate(workers, 1):
        task_ids = [task.task_id for task in worker]
        print(f"Worker {i}: {task_ids}")  # Display final worker assignments (TH)


if __name__ == "__main__":
    main()
