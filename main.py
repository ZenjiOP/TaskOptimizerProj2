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
    levels = task_list.topological_sort_by_levels()
    workers = []

    minimum_project_duration = find_min_duration(task_list)

    # Assign all tasks to workers
    for level, task_ids in enumerate(levels):
        if level == 0:
            for task_id in task_ids:
                worker = [task_list.tasks[task_id]]
                workers.append(worker)
        else:
            for task_id in task_ids:
                task = task_list.tasks[task_id]
                for worker in workers:
                    # Check if adding this task keeps the worker within the project duration
                    if sum(task_node.duration for task_node in worker) < minimum_project_duration:
                        worker.append(task)
                        break
                else:
                    # If no existing worker can take the task, create a new worker
                    workers.append([task])

    # Consolidate redundant workers
    workers = merge_workers(workers, minimum_project_duration)

    # Temporary print worker assignments
    for i, worker in enumerate(workers, 1):
        durations = [task.duration for task in worker]
        task_ids = [task.task_id for task in worker]
        print(f"Worker {i}: d:{durations}, t:{task_ids}")

    return workers


def merge_workers(workers, max_duration):
    final_workers = []  # List to hold the resulting merged workers
    current_merged_worker = []  # Temporary list to accumulate tasks
    current_duration = 0  # Tracks the accumulated duration of the current merged worker

    for worker in workers:
        # Skip this worker if any task has dependencies
        if any(task.dependencies for task in worker):
            final_workers.append(worker)
            continue

        for task in worker:
            # Check if adding the current task exceeds max_duration
            if current_duration + task.duration <= max_duration:
                current_merged_worker.append(task)
                current_duration += task.duration
            else:
                # Add the current merged worker to final_workers and reset for a new one
                if current_merged_worker:
                    final_workers.append(current_merged_worker)
                current_merged_worker = [task]  # Start a new merged worker with the current task
                current_duration = task.duration

    # Append any remaining tasks in the last accumulated worker
    if current_merged_worker:
        final_workers.append(current_merged_worker)

    return final_workers


def main():
    task_list = TaskListDAG()
    
    task_list.add_task(TaskNode(1, 10))
    task_list.add_task(TaskNode(2, 2))
    task_list.add_task(TaskNode(3, 2, [2]))
    task_list.add_task(TaskNode(4, 2, [2, 3]))
    task_list.add_task(TaskNode(5, 3))
    task_list.add_task(TaskNode(6, 2, [5]))
    task_list.add_task(TaskNode(7, 5))
    task_list.add_task(TaskNode(8, 2, [2, 3]))

    print("Minimum project duration:", find_min_duration(task_list))  # Calculate and print project duration
    workers = assign_workers(task_list)
    print("Final worker assignments:")
    for i, worker in enumerate(workers, 1):
        task_ids = [task.task_id for task in worker]
        print(f"Worker {i}: {task_ids}")  # Display final worker assignments


if __name__ == "__main__":
    main()
