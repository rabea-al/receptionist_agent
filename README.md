# Task Management System - Receptionist & Doer Agents

---

## Overview

This **Task Management System** consists of two primary agents:  
- **Receptionist Agent** - Manages task creation, retrieval, modification, and scheduling.  
- **Doer Agent** - Listens for tasks from the queue, executes them, and updates their status.
- **Timer File**
The system integrates with **RabbitMQ** for message passing, **SQLite** for task storage, and a **Timer component** to schedule task execution.

---

## Prerequisites

Before running the agents, ensure the following are installed and configured:

### **Required Libraries**
Install the necessary Xircuits libraries :
- `Agent`
- `SQLite`
- `Flask`

Install the necessary Python dependencies:
```bash
pip install -r requirement.txt
```


### **RabbitMQ Service**
- Use an external RabbitMQ provider such as ([CloudAMQP](https://www.cloudamqp.com/)) or a self-hosted RabbitMQ server.
- Configure the RabbitMQ connection in the system.

### Setting Up RabbitMQ on CloudAMQP

To use **RabbitMQ** with CloudAMQP, follow these steps to set up a free RabbitMQ server:

1. **Create an Account on CloudAMQP**  
   - Go to [CloudAMQP](https://www.cloudamqp.com/plans.html).
   - Choose the **Lemur** free plan to create a new RabbitMQ server.
   - Once the server is created, you will receive connection details.

2. **Configure the Connection to RabbitMQ**  
   - Use the provided CloudAMQP credentials (`broker URL`, `port`, `username`, `password`, and `virtual host`).
   - Ensure that the **virtual host** is correctly set when connecting to the server.

3. **Set Up Exchange and Bind to Queues**  
   - Open the **RabbitMQ Management Console** via your CloudAMQP account.
   - Navigate to the **Exchanges** section and create a new **Direct Exchange** or use the default `amq.direct` exchange.
   - Go to the **Queues** section and create a queue to receive messages.
   - Bind the queue to the exchange using an appropriate **Routing Key**.

4. **Ensure Messages Are Published to an Exchange**  
   - In RabbitMQ, messages are published to an **Exchange**, not directly to **Queues**.
   - The exchange distributes messages to the queues bound to it based on the **Routing Key**.

After completing these steps, you can use RabbitMQ to publish and consume messages in your agents without needing to run a local RabbitMQ server.

---

## How to Run the Agents
You need to run these agents on the XpressAI platform to interact with the Receptionist Agent.

### **1. Start the Receptionist Agent**
You need to create the **Receptionist Agent** on the XpressAI platform as an agent to be able to talk to it and add tasks to the database.

### **2. Start the Timer.py file**

```bash
   python timer.py
   ```
   Start the Flask for scheduling (and Timer triggering) this will set up the RabbitMQ connection.
   The **Timer** runs every minute and:
- Checks the task database for tasks where the **execution time has arrived**.
- Ensures the task is **active** (`is_active = 1`) and **not deferred** (`is_waiting = 0`).
- Sends the task ID to RabbitMQ for execution.


### **3. Start the Doer Agent**
```bash
python doer_agent.py
```
This agent:
- Listens to the RabbitMQ queue for new task IDs.
- Fetches task details and executes the task.
- Updates the database to mark tasks as completed.




---

## **How the Agents Work**

### **Receptionist Agent**
Handles task lifecycle management with the following functionalities:
- **Create a new task** with a unique `task_id`, description, and execution time.
- **Fetch task details** based on a given `task_id`.
- **Delete tasks** when no longer needed.
- **Defer tasks** (mark `is_waiting = 1`).
- **Resume tasks** (mark `is_waiting = 0`).

### **Doer Agent**
Executes the tasks:
- **Retrieves task ID** from the queue.
- **Retrieves task details** from the database.
- **Executes tasks**, such as fetching weather data.
- **Marks tasks as complete** (`is_active = 0`).

---

## **Example Task Execution Flow**

1. **User Creates a Task**
   ```json
   {
     "task_id": "223",
     "summary": "Fetch Weather Report",
     "details": "Retrieve the current weather forecast for Dubai.",
     "steps": ["Retrieve weather data", "Analyze temperature", "Analyze humidity"],
     "execution_time": "2025-02-28T02:08:00"
   }
   ```
   
2. **Timer Checks and Sends Task ID to Queue**
   - If execution time matches the current time, the task ID is sent to RabbitMQ.

3. **Doer Agent Fetches and Executes Task**
   - The Doer retrieves task `223` from the database.
   - Executes the task (e.g., retrieves weather data).
   - Updates the database (`is_active = 0`).

---

### **Note**
- If the database is locked (`database is locked` error), the system will retry the operation with a short delay.
- Tasks are stored with `execution_time` formatted as `YYYY-MM-DD HH:MM` (ignoring seconds).


