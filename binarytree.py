class TaskNode:
    """Class representing a node in the binary tree for tasks."""
    def __init__(self, priority, task, person):
        self.priority = priority  # Priority of the task
        self.task = task          # Task description
        self.person = person      # Person assigned to the task
        self.left = None          # Left child (lower priority)
        self.right = None         # Right child (higher priority)

class TaskOrganizer:
    """Class to manage the binary tree of tasks."""
    def __init__(self):
        self.root = None  # Root of the binary tree

    def add_task(self, priority, task, person):
        """Add a task to the binary tree."""
        new_node = TaskNode(priority, task, person)
        if not self.root:
            self.root = new_node  # If tree is empty, set new task as root
        else:
            self._add_task_recursive(self.root, new_node)

    def _add_task_recursive(self, current, new_node):
        """Recursive helper to add a task to the correct position in the tree."""
        if new_node.priority < current.priority:
            if current.left:
                self._add_task_recursive(current.left, new_node)
            else:
                current.left = new_node
        else:
            if current.right:
                self._add_task_recursive(current.right, new_node)
            else:
                current.right = new_node

    def display_tasks(self):
        """Display all tasks in ascending order of priority."""
        print("\nTask List by Priority:")
        self._in_order_traversal(self.root)

    def _in_order_traversal(self, node):
        """Recursive helper to perform in-order traversal of the tree."""
        if node:
            self._in_order_traversal(node.left)
            print(f"Priority: {node.priority}, Task: {node.task}, Assigned to: {node.person}")
            self._in_order_traversal(node.right)

    def find_tasks_for_person(self, person):
        """Find and return tasks assigned to a specific person."""
        tasks = []
        self._find_tasks_for_person_recursive(self.root, person, tasks)
        return tasks

    def _find_tasks_for_person_recursive(self, node, person, tasks):
        """Recursive helper to find tasks assigned to a person."""
        if node:
            if node.person == person:
                tasks.append((node.priority, node.task))
            self._find_tasks_for_person_recursive(node.left, person, tasks)
            self._find_tasks_for_person_recursive(node.right, person, tasks)

def main():
    """Main function to provide a user interface for the task organizer."""
    organizer = TaskOrganizer()

    while True:
        print("\n--- Task Organizer Menu ---")
        print("1. Add a Task")
        print("2. View All Tasks")
        print("3. Find Tasks for a Person")
        print("4. Exit")
        choice = input("Enter your choice: ")

        if choice == "1":
            # Add a task
            priority = int(input("Enter task priority (1 = Highest, higher number = lower priority): "))
            task = input("Enter task description: ")
            person = input("Enter the name of the person assigned to this task: ")
            organizer.add_task(priority, task, person)
            print("Task added successfully!")

        elif choice == "2":
            # View all tasks
            organizer.display_tasks()

        elif choice == "3":
            # Find tasks for a specific person
            person = input("Enter the name of the person to find their tasks: ")
            tasks = organizer.find_tasks_for_person(person)
            if tasks:
                print(f"\nTasks for {person}:")
                for priority, task in tasks:
                    print(f"Priority: {priority}, Task: {task}")
            else:
                print(f"No tasks found for {person}.")

        elif choice == "4":
            # Exit the program
            print("Exiting the Task Organizer. Goodbye!")
            break

        else:
            print("Invalid choice! Please try again.")

if __name__ == "__main__":
    main()