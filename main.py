import os
import csv
import copy

## Fallback for just in case I don't get something more creative working before class --- rename file to main.py before running ---

class TaskNode:
    def __init__(self, task_id, duration, main_task=None):
        self.task_id = task_id
        self.duration = duration
        self.main_task = main_task if main_task else None

        self.min_start_time = 0 if not main_task else main_task.duration


def load_data(task_list, file_name):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_csv = os.path.join(current_dir, file_name)

    with open(data_csv, mode='r') as file:
        reader = csv.reader(file)
        next(reader)  # skip header

        for row in reader:
            task_id = row[0].strip()

            # Original task with its duration
            duration = int(row[2].strip())
            new_task = TaskNode(task_id, duration)
            task_list.append(new_task)

            # Create dependent tasks for each element in column 2
            if row[1]:  # If column 2 is not empty
                dependencies = [
                    dep.strip() for dep in row[1].split(',')
                ]

                for dep in dependencies:
                    dep_task_id = f"{dep}(MT[{task_id}])"
                    task_list.append(TaskNode(dep_task_id, 1, new_task))

    return task_list


def find_min_duration(task_list):
    latest_task = max(task_list, key=lambda task: task.min_start_time + task.duration)
    return latest_task.min_start_time + latest_task.duration


def assign_workers(task_list):
    workers = []
    min_project_duration = find_min_duration(task_list)
    remaining_tasks = copy.copy(task_list)  # Work with a copy of task_list

    for task in remaining_tasks:
        if task.main_task is None:
            workers.append([task])

    for worker in workers:
        changes_made = True
        while changes_made:
            changes_made = False
            for task in remaining_tasks[:]:  # Iterate safely with task removal
                if task.main_task is None:
                    continue  # Skip tasks without a main_task

                work_duration = sum(t.duration for t in worker)

                # Check if task can be added to this worker
                if work_duration >= task.min_start_time and work_duration + task.duration <= min_project_duration:
                    worker.append(task)
                    remaining_tasks.remove(task)  # Remove from the working copy
                    changes_made = True

    workers = merge_workers(workers, task_list)

    return workers


def merge_workers(workers, task_list):
    min_project_duration = find_min_duration(task_list)

    for task in task_list:
        print(f"{task.task_id}")

    # Identify safe tasks and valid workers
    tasks_with_dep = {task.main_task for task in task_list if task.main_task}
    safe_tasks = {task for task in task_list if task not in tasks_with_dep and task.main_task is None}
    valid_workers = [worker for worker in workers if len(worker) == 1 and worker[0] in safe_tasks]
    invalid_workers = [worker for worker in workers if worker not in valid_workers]

    print(f"Dependent Tasks: {[task.task_id for task in tasks_with_dep]}")
    print(f"Valid workers: {[[task.task_id for task in worker] for worker in valid_workers]}")
    print(f"Invalid workers: {[[task.task_id for task in worker] for worker in invalid_workers]}")
    print(f"Total IV workers: {len(invalid_workers) + len(valid_workers)}")

    # merge valid workers into invalid workers while respecting min_project_duration
    merged_tasks = set()  # To track tasks already merged

    for vw in valid_workers:
        task_to_merge = vw[0]

        if task_to_merge in merged_tasks:  # Skip if already merged
            continue

        for ivw in invalid_workers:
            work_duration = sum(task_node.duration for task_node in ivw)

            if work_duration + task_to_merge.duration <= min_project_duration:
                ivw.append(task_to_merge)
                merged_tasks.add(task_to_merge)  # Mark as merged
                break  # Stop after successfully merging this task

    # Another pass now looking to merge invalid workers in a way in which DT's are still respected?

    return invalid_workers


def main():
    task_list = []

    task_list = load_data(task_list, 'Task_Dependencies_and_Time_Dataframe_Revised.csv')
    task_list.sort(key=lambda task: task.min_start_time)

    print("Minimum project duration:", find_min_duration(task_list))  # Calculate and print project duration
    workers = assign_workers(task_list)
    print("Final worker assignments:")
    for i, worker in enumerate(workers, 1):
        completion_times = [task.min_start_time + task.duration for task in worker]

        rolling_total = 0  # Initialize rolling total
        task_info = []  # To store formatted task strings

        for task, completion_time in zip(worker, completion_times):
            task_info.append(f"{task.task_id}({completion_time})T:({rolling_total})")
            rolling_total += task.duration  # Add current task's duration to rolling total

        task_info_str = ', '.join(task_info)  # Join all formatted tasks into a string

        total_work = sum(task.duration for task in worker)
        print(f"Worker {i}: {task_info_str}: Total Work: {total_work}")


if __name__ == "__main__":
    main()
