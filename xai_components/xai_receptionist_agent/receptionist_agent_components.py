from xai_components.base import InArg, InCompArg, OutArg, Component, xai_component
from datetime import datetime
import time
import ssl
import pika
import sqlite3
import json

@xai_component
class TasksOpenDB(Component):
    """Opens or creates a SQLite database with the proper schema for task management.

    ##### inPorts:
    - db_file: Path to the SQLite database file

    ##### outPorts:
    - connection: SQLite database connection object
    """
    db_file: InCompArg[str]
    connection: OutArg[sqlite3.Connection]

    def execute(self, ctx) -> None:
        conn = sqlite3.connect(self.db_file.value, check_same_thread=False)
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT UNIQUE NOT NULL,
                summary TEXT NOT NULL,
                conversation TEXT,
                details TEXT,
                steps TEXT,
                execution_time TEXT,
                current_step_num INTEGER DEFAULT 0,
                is_active BOOLEAN DEFAULT 1,
                is_waiting BOOLEAN DEFAULT 0
            )
        ''')
        conn.commit()
        self.connection.value = conn
        ctx['tasksdb_conn'] = conn

@xai_component
class TasksCreateTask(Component):
    """Creates a new task in the database with the specified details.

    ##### inPorts:
    - connection: SQLite database connection.
    - task_id: (Optional) The ID of the task to create. If not provided, a unique value will be generated automatically.
    - summary: Brief description of the task.
    - conversation: List of conversation history related to the task.
    - details: Detailed description of the task.
    - steps: List of steps to complete the task.
    - execution_time: The time when the task should be executed (if not provided, defaults to now).

    ##### outPorts:
    - task_id_out: The task_id of the newly created task.
    - result: A text message indicating the success of the task creation.
    """

    connection: InArg[sqlite3.Connection]
    task_id: InCompArg[str]  # Optional user-provided task ID as a string
    summary: InCompArg[str]
    conversation: InCompArg[list]
    details: InCompArg[str]
    steps: InCompArg[list]
    execution_time: InCompArg[str]  # Expected in ISO format
    task_id_out: OutArg[str]  # Output task_id as string
    result: OutArg[str]       # Result message

    def execute(self, ctx) -> None:
        conn = self.connection.value if self.connection.value is not None else ctx['tasksdb_conn']

        if self.execution_time.value:
            exec_time = datetime.fromisoformat(self.execution_time.value).strftime("%Y-%m-%d %H:%M")
        else:
            exec_time = datetime.now().strftime("%Y-%m-%d %H:%M")

        provided_id = self.task_id.value.strip() if self.task_id.value and self.task_id.value.strip() != "" else None
        if not provided_id:
            provided_id = str(uuid.uuid4())

        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO tasks (task_id, summary, conversation, details, steps, execution_time)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                provided_id,
                self.summary.value,
                json.dumps(self.conversation.value),
                self.details.value,
                json.dumps(self.steps.value),
                exec_time
            ))
        except sqlite3.IntegrityError as e:
            self.result.value = f" Error: The provided task ID '{provided_id}' is already in use. {e}"
            return
        conn.commit()
        self.task_id_out.value = provided_id
        self.result.value = f" Task with ID {provided_id} created successfully."

@xai_component
class TasksGetTaskDetails(Component):
    """
    Retrieves all details of a specific task by its task_id.

    ##### inPorts:
    - connection: SQLite database connection.
    - task_id: Task ID as a JSON string (expects {"task_id": "some_id"}) or as a dict or as an int.

    ##### outPorts:
    - task_details: A formatted string containing task details.
    - summary: Brief description of the task.
    - conversation: List of conversation history.
    - details: Detailed description of the task.
    - steps: List of task steps.
    - execution_time: The execution time as a string.
    - current_step_num: Current step number.
    - is_active: Whether the task is active.
    - is_waiting: Whether the task is waiting.
    """

    connection: InArg[sqlite3.Connection]
    task_id: InCompArg[str]
    task_details: OutArg[str]
    summary: OutArg[str]
    conversation: OutArg[list]
    details: OutArg[str]
    steps: OutArg[list]
    execution_time: OutArg[str]
    current_step_num: OutArg[int]
    is_active: OutArg[bool]
    is_waiting: OutArg[bool]

    def execute(self, ctx) -> None:
        conn = self.connection.value if self.connection.value is not None else ctx['tasksdb_conn']
        cursor = conn.cursor()

        print(f"Received task_id input: {self.task_id.value}")

        if isinstance(self.task_id.value, int):
            task_data = {"task_id": self.task_id.value}
        elif isinstance(self.task_id.value, dict):
            task_data = self.task_id.value
        elif isinstance(self.task_id.value, str):
            try:
                task_data = json.loads(self.task_id.value)
            except (ValueError, json.JSONDecodeError) as e:
                try:
                    task_data = {"task_id": int(self.task_id.value.strip())}
                except Exception as ex:
                    print(f"Error parsing task_id from string. Received: {self.task_id.value}. Error: {ex}")
                    self.task_details.value = None
                    return
        else:
            print(f"Unsupported type for task_id: {type(self.task_id.value)}")
            self.task_details.value = None
            return

        task_id_val = task_data.get("task_id", None)
        if task_id_val is None:
            print(f"task_id key not found in input: {task_data}")
            self.task_details.value = None
            return

        task_id_text = str(task_id_val).strip()

        cursor.execute('''
            SELECT task_id, summary, conversation, details, steps, execution_time, current_step_num, is_active, is_waiting
            FROM tasks
            WHERE task_id = ?
        ''', (task_id_text,))
        row = cursor.fetchone()
        if row:
            try:
                conversation_data = json.loads(row[2]) if row[2] else []
            except Exception:
                conversation_data = []
            try:
                steps_data = json.loads(row[4]) if row[4] else []
            except Exception:
                steps_data = []

            formatted_details = (
                f"Task ID: {row[0]}\n"
                f"Summary: {row[1]}\n"
                f"Details: {row[3]}\n"
                f"Steps: {', '.join(map(str, steps_data))}\n"
                f"Execution Time: {row[5]}\n"
                f"Current Step Number: {row[6]}\n"
                f"Is Active: {row[7]}\n"
                f"Is Waiting: {row[8]}"
            )
            self.task_details.value = formatted_details

            self.summary.value = row[1]
            self.conversation.value = conversation_data
            self.details.value = row[3]
            self.steps.value = steps_data
            self.execution_time.value = row[5]
            self.current_step_num.value = row[6]
            self.is_active.value = row[7]
            self.is_waiting.value = row[8]

            print(f"Task {task_id_text} details retrieved successfully.")
        else:
            print(f"No task found with id: {task_id_text}")
            self.task_details.value = f"No task found with ID {task_id_text}."

@xai_component
class TasksDeleteTask(Component):
    """Deletes a task from the database.

    ##### inPorts:
    - connection: SQLite database connection.
    - task_id: ID of the task to delete.

    ##### outPorts:
    - result: A text message indicating the result of the deletion.
    """

    connection: InArg[sqlite3.Connection]
    task_id: InCompArg[str]
    result: OutArg[str]

    def execute(self, ctx) -> None:
        conn = self.connection.value if self.connection.value is not None else ctx['tasksdb_conn']
        cursor = conn.cursor()
        cursor.execute('DELETE FROM tasks WHERE task_id = ?', (self.task_id.value,))
        conn.commit()
        self.result.value = f"Task with ID {self.task_id.value} deleted successfully."

@xai_component
class TasksUpdateTask(Component):
    """Updates an existing task's details.

    ##### inPorts:
    - connection: SQLite database connection
    - task_id: ID of the task to update
    - summary: New summary text
    - conversation: Updated conversation history
    - details: New detailed description
    - steps: Updated list of steps
    """

    connection: InArg[sqlite3.Connection]
    task_id: InCompArg[str]
    summary: InArg[str]
    conversation: InArg[dict]
    details: InArg[str]
    steps: InArg[list]

    def execute(self, ctx) -> None:
        conn = self.connection.value if self.connection.value is not None else ctx['tasksdb_conn']
        cursor = conn.cursor()

        cursor.execute('SELECT task_id, summary, conversation, details, steps FROM tasks WHERE task_id = ?', (self.task_id.value,))
        row = cursor.fetchone()
        if row:
            summary = self.summary.value if self.summary.value is not None else row[1]
            conversation = self.conversation.value if self.conversation.value is not None else json.loads(row[2])
            details = self.details.value if self.details.value is not None else row[3]
            steps = self.steps.value if self.steps.value is not None else json.loads(row[4])

            cursor.execute('''
                UPDATE tasks
                SET summary = ?, conversation = ?, details = ?, steps = ?
                WHERE task_id = ?
            ''', (summary, json.dumps(conversation), details, json.dumps(steps), self.task_id.value))
        conn.commit()

@xai_component
class TasksListActiveTasks(Component):
    """Retrieves a list of all active tasks from the database.

    ##### inPorts:
    - connection: SQLite database connection

    ##### outPorts:
    - active_tasks: List of dictionaries containing active task details
    """

    connection: InArg[sqlite3.Connection]
    active_tasks: OutArg[list]  # Output list of active tasks

    def execute(self, ctx) -> None:
        conn = self.connection.value if self.connection.value is not None else ctx['tasksdb_conn']
        cursor = conn.cursor()
        cursor.execute('SELECT task_id, summary, conversation, details, steps, current_step_num, is_active, is_waiting FROM tasks WHERE is_active = 1')
        rows = cursor.fetchall()
        self.active_tasks.value = [{
            'task_id': row[0],
            'summary': row[1],
            'conversation': json.loads(row[2]),
            'details': row[3],
            'steps': json.loads(row[4]),
            'current_step_num': row[5],
            'is_active': row[6],
            'is_waiting': row[7]
        } for row in rows]

@xai_component
class TasksCompleteTask(Component):
    """Marks a task as completed by setting is_active to false.

    ##### inPorts:
    - connection: SQLite database connection.
    - task_id: ID of the task to complete.

    ##### outPorts:
    - result: A text message indicating the result of marking the task as completed.
    """

    connection: InArg[sqlite3.Connection]
    task_id: InCompArg[str]
    result: OutArg[str]

    def execute(self, ctx) -> None:
        conn = self.connection.value if self.connection.value is not None else ctx['tasksdb_conn']
        cursor = conn.cursor()

        max_retries = 5
        retry_delay = 0.5
        for attempt in range(max_retries):
            try:
                cursor.execute('UPDATE tasks SET is_active = 0 WHERE task_id = ?', (self.task_id.value,))
                conn.commit()
                break
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e):
                    print(f"Database is locked. Retrying in {retry_delay} seconds (Attempt {attempt+1}/{max_retries})...")
                    time.sleep(retry_delay)
                else:
                    raise

        if cursor.rowcount > 0:
            self.result.value = f"Task with ID {self.task_id.value} has been marked as completed."
        else:
            self.result.value = f"Task with ID {self.task_id.value} not found or already completed."

@xai_component
class TasksDeferTask(Component):
    """Marks a task as waiting.

    ##### inPorts:
    - connection: SQLite database connection
    - task_id: ID of the task to defer
    """

    connection: InArg[sqlite3.Connection]
    task_id: InCompArg[str]

    def execute(self, ctx) -> None:
        conn = self.connection.value if self.connection.value is not None else ctx['tasksdb_conn']
        cursor = conn.cursor()
        cursor.execute('UPDATE tasks SET is_waiting = 1 WHERE task_id = ?', (self.task_id.value,))
        conn.commit()

@xai_component
class TasksResumeTask(Component):
    """Resumes a waiting task by setting is_waiting to false.

    ##### inPorts:
    - connection: SQLite database connection
    - task_id: ID of the task to resume
    """

    connection: InArg[sqlite3.Connection]
    task_id: InCompArg[str]

    def execute(self, ctx) -> None:
        conn = self.connection.value if self.connection.value is not None else ctx['tasksdb_conn']
        cursor = conn.cursor()
        cursor.execute('UPDATE tasks SET is_waiting = 0 WHERE task_id = ?', (self.task_id.value,))
        conn.commit()


@xai_component
class TasksCloseDB(Component):
    """Closes the SQLite database connection.

    ##### inPorts:
    - connection: SQLite database connection to close
    """

    connection: InArg[sqlite3.Connection]

    def execute(self, ctx) -> None:
        conn = self.connection.value if self.connection.value is not None else ctx['tasksdb_conn']
        conn.close()

@xai_component
class Timer(Component):
    """
    This component checks the task database for tasks that are ready to be executed in the current minute.
    It is triggered on demand (e.g., via a Flask endpoint) instead of running every minute.

    ##### inPorts:
    - connection: Open SQLite database connection.

    ##### outPorts:
    - sent_task_id: A JSON string containing the task_id of the task ready for execution.
    """
    connection: InArg[sqlite3.Connection]
    sent_task_id: OutArg[str]

    def execute(self, ctx) -> None:
        conn = self.connection.value if self.connection.value is not None else ctx["tasksdb_conn"]
        cursor = conn.cursor()

        # Get the current time in the correct format (YYYY-MM-DD HH:MM)
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        print("Timer (on demand): Checking tasks at", now)

        cursor.execute("""
            SELECT task_id FROM tasks
            WHERE execution_time <= ?
            AND is_active = 1
            AND is_waiting = 0
        """, (now,))

        tasks = cursor.fetchall()

        if tasks:
            for task in tasks:
                task_id = task[0]
                print(f"Timer (on demand): Task {task_id} is ready for execution.")
                self.sent_task_id.value = json.dumps({"task_id": task_id})
                # Optionally, update the task status here to prevent re-sending.
        else:
            print("Timer (on demand): No active tasks are ready for execution.")

@xai_component()
class ExtractTaskDetails(Component):
    """
    Component to extract task details from a JSON string.

    Expected JSON format:
    {
        "task_id": "123",  // Optional
        "summary": "Brief description of the task",
        "details": "Detailed description of the task",
        "steps": ["Step1", "Step2", "Step3"],
        "execution_time": "2025-02-26T12:00:00"  // Optional: ISO formatted time; defaults to current time if not provided.
    }

    ##### inPorts:
    - input_json: JSON string containing the task details.

    ##### outPorts:
    - task_id: The ID of the task (if provided).
    - summary: The task summary.
    - details: Detailed description of the task.
    - steps: List of steps required to complete the task.
    - execution_time: The time when the task should be executed.
    """
    input_json: InArg[str]

    task_id: OutArg[str]
    summary: OutArg[str]
    details: OutArg[str]
    steps: OutArg[list]
    execution_time: OutArg[str]

    def execute(self, ctx) -> None:
        input_data = json.loads(self.input_json.value)

        self.task_id.value = str(input_data.get("task_id", ""))
        self.summary.value = str(input_data.get("summary", ""))
        self.details.value = str(input_data.get("details", ""))
        self.steps.value = input_data.get("steps", [])
        self.execution_time.value = str(input_data.get("execution_time", ""))

        print(f"Task ID: {self.task_id.value}")
        print(f"Task Summary: {self.summary.value}")
        print(f"Details: {self.details.value}")
        print(f"Steps: {self.steps.value}")
        print(f"Execution Time: {self.execution_time.value}")

@xai_component
class RabbitMQConnect(Component):
    broker: InArg[str]
    port: InArg[int]
    username: InArg[str]
    password: InArg[str]
    vhost: InArg[str]

    def execute(self, ctx) -> None:
        ssl_context = ssl.create_default_context()

        credentials = pika.PlainCredentials(self.username.value, self.password.value)
        parameters = pika.ConnectionParameters(
            host=self.broker.value,
            port=self.port.value,
            virtual_host=self.vhost.value if self.vhost.value is not None else '/',
            credentials=credentials,
            ssl_options=pika.SSLOptions(ssl_context)
        )

        client = pika.BlockingConnection(parameters)
        ctx['rabbitmq_client'] = client
        ctx['rabbitmq_channel'] = client.channel()

@xai_component
class RabbitMQPublish(Component):
    queue: InArg[str]
    routing_key: InArg[str]
    exchange: InArg[str]
    message: InArg[str]

    def execute(self, ctx) -> None:
        channel = ctx['rabbitmq_channel']

        if ctx.get('rabbitmq_queue') is None:
            channel.queue_declare(queue=self.queue.value)
            ctx['rabbitmq_queue'] = self.queue.value

        exchange = '' if self.exchange.value is None else self.exchange.value
        routing_key = '' if self.routing_key.value is None else self.routing_key.value

        channel.basic_publish(exchange=exchange, routing_key=routing_key, body=self.message.value)


@xai_component
class RabbitMQConsume(Component):
    on_message: BaseComponent
    queue: InArg[str]
    exchange: InArg[str]
    routing_key: InArg[str]
    message: OutArg[str]
    conversation: OutArg[list]

    def execute(self, ctx) -> None:
        channel = ctx['rabbitmq_channel']

        if ctx.get('rabbitmq_queue') is None:
            channel.queue_declare(queue=self.queue.value)
            ctx['rabbitmq_queue'] = self.queue.value

        channel.basic_consume(
            queue=self.queue.value,
            on_message_callback=lambda ch, meth, prop, body: self.process_message(ctx, ch, meth, prop, body))

    def process_message(self, ctx, channel, method, properties, body):
        self.message.value = body.decode('utf-8')
        ctx['rabbitmq_message'] = body
        ctx['rabbitmq_properties'] = properties

        self.conversation.value = [
            {"role": "user", "content": self.message.value}
        ]
        ctx['conversation'] = self.conversation.value

        self.on_message.do(ctx)

        channel.basic_ack(delivery_tag=method.delivery_tag)


@xai_component
class RabbitMQStartConsuming(Component):

    def execute(self, ctx) -> None:
        channel = ctx['rabbitmq_channel']

        try:
            channel.start_consuming()
        except Exception as e:
            print(e)


@xai_component
class RabbitMQDisconnect(Component):

    def execute(self, ctx) -> None:
        client = ctx['rabbitmq_client']

        try:
            client.close()
        except Exception as e:
            print(e)

@xai_component
class RabbitMQPurgeQueue(Component):
    """
    Purges all messages from a RabbitMQ queue and retries if needed.

    ##### inPorts:
    - broker: The RabbitMQ broker URL.
    - port: The RabbitMQ broker port.
    - username: Username for authentication.
    - password: Password for authentication.
    - vhost: Virtual host to connect to.
    - queue: The name of the queue to purge.

    ##### outPorts:
    - None
    """

    broker: InArg[str]
    port: InArg[int]
    username: InArg[str]
    password: InArg[str]
    vhost: InArg[str]
    queue: InArg[str]

    def execute(self, ctx) -> None:
        retries = 3  #
        for attempt in range(retries):
            try:
                print(f"🔄 Attempt {attempt+1} to purge queue: {self.queue.value}")

                # Create SSL context
                ssl_context = ssl.create_default_context()

                # Establish connection to RabbitMQ
                credentials = pika.PlainCredentials(self.username.value, self.password.value)
                parameters = pika.ConnectionParameters(
                    host=self.broker.value,
                    port=self.port.value,
                    virtual_host=self.vhost.value,
                    credentials=credentials,
                    ssl_options=pika.SSLOptions(ssl_context)
                )
                connection = pika.BlockingConnection(parameters)
                channel = connection.channel()

                # Purge the queue
                channel.queue_purge(queue=self.queue.value)
                print(f"Successfully purged queue: {self.queue.value}")

                # Close connection
                connection.close()
                break

            except pika.exceptions.StreamLostError:
                print(f"StreamLostError: Retrying in 2 seconds...")
                time.sleep(2)
            except Exception as e:
                print(f"Error purging queue: {e}")
                break
