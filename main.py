from collections import defaultdict


class TaskNode:
    # Class representing each individual task
    def __init__(self, dependencies, duration):
        self.dependencies = dependencies
        self.duration = duration

        # calculated by the graph
        self.start_time = None
        self.end_time = None


# DAG to contain all task nodes and their dependencies
class TaskListDAG:
    def __init__(self):
        self.graph = defaultdict(list)  # List of dependencies for each task
        self.tasks = {}  # List of all tasks in the DAG

    # Add task to the DAG
    def add_task(self, task_id, task_node):
        if task_id in self.tasks:  # Duplicate task catch
            raise ValueError(f"Task '{task_id}' already exists in the DAG")

        self.tasks[task_id] = task_node

        # Add edges to the graph to represent dependencies
        for dependency in task_node.dependencies:
            if dependency not in self.tasks:
                raise ValueError(f"Dependency {dependency} for task {task_id} not found")
            self.graph[dependency].append(task_id)

        if task_id not in self.graph:
            self.graph[task_id] = []

    def print_graph(self):
        for node in self.graph:
            print(f"{node} -> {self.graph[node]}")


def topological_sort_helper(node, visited, stack, graph):
    # Mark the current node as visited
    visited.add(node)

    # Recur for all neighbors of the current node
    for neighbor in graph[node]:
        if neighbor not in visited:
            topological_sort_helper(neighbor, visited, stack, graph)

    # Push the current node to the stack (topological order)
    stack.append(node)


# Topologically sort the DAG to provide the order in which to complete tasks
def topological_sort(tasks):
    visited = set()
    stack = []

    # Call the recursive helper function for all nodes
    for node in tasks.graph:
        if node not in visited:
            topological_sort_helper(node, visited, stack, tasks.graph)

    # Return the stack in reverse order for topological sorting
    return stack[::-1]


# Calculate all task start and end time based on dependencies
def calc_start_end_time(tasks, topo_order):
    for task_id in topo_order:
        task_node = tasks.tasks[task_id]

        if task_node.dependencies:
            task_node.start_time = max(tasks.tasks[dep].end_time for dep in task_node.dependencies)
        else:
            task_node.start_time = 0

        task_node.end_time = task_node.start_time + task_node.duration


# Returns the minimum duration to complete all tasks
def find_min_duration(tasks):
    return max(task_node.end_time for task_node in tasks.tasks.values())


def main():
    task_list = TaskListDAG()

    task_list.add_task("A", TaskNode(dependencies=[], duration=3))
    task_list.add_task("B", TaskNode(dependencies=["A"], duration=1))
    task_list.add_task("C", TaskNode(dependencies=["B"], duration=1))
    task_list.add_task("D", TaskNode(dependencies=[], duration=3))

    calc_start_end_time(task_list, topological_sort(task_list))

    for task_id, task_node in task_list.tasks.items():
        print(f"Task {task_id}: Start={task_node.start_time}, End={task_node.end_time}")

    print(f"Minimum total task completion time: {find_min_duration(task_list)}")


if __name__ == "__main__":
    main()
