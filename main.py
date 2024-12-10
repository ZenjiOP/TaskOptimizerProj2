from collections import deque, defaultdict


class TaskNode:
    # Class representing each individual task
    def __init__(self, task_id, dependencies, duration):
        self.task_id = task_id
        self.dependencies = dependencies
        self.duration = duration

        # calculated by the graph
        self.start_time = None
        self.end_time = None


class TaskListDAG:
    def __init__(self):
        self.graph = defaultdict(list)  # Adjacency list for dependencies
        self.tasks = {}

    # Add a task to the DAG
    def add_task(self, task_node):
        task_id = task_node.task_id
        if task_id in self.tasks:
            raise ValueError(f"Task '{task_id}' already exists in the DAG")

        self.tasks[task_id] = task_node

        # Add edges to represent dependencies
        for dependency in task_node.dependencies:
            if dependency not in self.tasks:
                raise ValueError(f"Dependency {dependency} for task {task_id} not found")
            self.graph[dependency].append(task_id)

        if task_id not in self.graph:
            self.graph[task_id] = []

    def print_graph(self):
        for node in self.graph:
            print(f"{node} -> {self.graph[node]}")

    # Topological sort by levels
    def topological_sort_by_levels(self):
        # Calculate in-degrees for all tasks
        in_degree = {task: 0 for task in self.tasks}
        for task in self.graph:
            for neighbor in self.graph[task]:
                in_degree[neighbor] += 1

        # Initialize queue with nodes having in-degree 0
        queue = deque(sorted([task for task in self.tasks if in_degree[task] == 0]))
        levels = []

        # Process nodes level by level
        while queue:
            level = []
            for _ in range(len(queue)):  # Process all nodes at the current level
                current_task = queue.popleft()
                level.append(current_task)

                for neighbor in sorted(self.graph[current_task]):
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)

            levels.append(level)  # Add the current level to the result

        # Check for cycles
        if sum(len(level) for level in levels) != len(self.tasks):
            raise ValueError("Cycle detected in the DAG")

        return levels


# Find the minimum time to finish the project based on the length of the longest task and update start/end times
def find_min_duration(tasks):
    levels = tasks.topological_sort_by_levels()

    for level in levels:
        for task_id in level:
            task_node = tasks.tasks[task_id] 

            if task_node.dependencies:
                # Calculate the start time as the maximum end time of dependencies
                task_node.start_time = max(tasks.tasks[dep].end_time for dep in task_node.dependencies)
            else:
                # No dependencies, start immediately
                task_node.start_time = 0

            # Calculate end time
            task_node.end_time = task_node.start_time + task_node.duration

    # Return the maximum end_time across all tasks
    return max(task.end_time for task in tasks.tasks.values())


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
        print(f"Worker {i}: {durations}, {task_ids}")
    
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

    task_list.add_task(TaskNode("A", dependencies=[], duration=10))
    task_list.add_task(TaskNode("B", dependencies=[], duration=2))
    task_list.add_task(TaskNode("C", dependencies=["B"], duration=2))
    task_list.add_task(TaskNode("D", dependencies=["B", "C"], duration=2))
    task_list.add_task(TaskNode("E", dependencies=[], duration=3))
    task_list.add_task(TaskNode("F", dependencies=["E"], duration=2))
    task_list.add_task(TaskNode("G", dependencies=[], duration=5))
    task_list.add_task(TaskNode("H", dependencies=["B", "C"], duration=2))

    task_list.print_graph()

    print(f"Minimum total task completion time: {find_min_duration(task_list)}")
    print(task_list.topological_sort_by_levels())
    assign_workers(task_list)


if __name__ == "__main__":
    main()
