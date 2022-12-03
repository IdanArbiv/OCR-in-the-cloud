# OCR in the Cloud

# Introduction 
The application is composed of a local application and instances running on the Amazon cloud. The application will get as an input a text file containing a list of URLs of images. Then, instances will be launched in AWS (workers). Each worker will download image files, use some OCR library to identify texts in those image and display the image with the text in a webpage. The use-case is as follows:
1. User starts the application and supplies as input a file with URLs of image files, an integer n stating how many files a single worker can process, and an optional argument terminate, if received the local application sends a terminate message to the Manager.
2. User gets back an output html file with images and their texts.

# Dependencies
ami: ami-00e95a9222311e8ed
type: large
Load POM.xml

# Short usage tutorial
1.	Extract the project files into 3 separate project folders - Local, Manager, Worker.
2.	Load the dependency by loading the Pom.xml of each project.
3.	Connect to the AWS account and copy the credentials
4.	Set credentials in the AWS credentials profile file on your local system, located at:
    ~/.aws/credentials on Linux, macOS, or Unix
    C:\Users\USERNAME\.aws\credentials on Windows
5.	Under the AWS classes change the constants concerning REGIONS according to the REGIONS given to you in AWS.
Const we used:
    S3 Region: US_WEST_2
    SQS Region: US_WEST_2
    EC2 Region: US_EAST_1
    ![image](https://user-images.githubusercontent.com/101040591/205453495-0b1e882b-bb21-496f-8bdd-245fafea0779.png)

6.	Extract the 2 jar files. One for the manager's project and one for the worker's project.
7.	Under "LocalAppProject" under the App class change the constants MANAGER_SERVICE_JAR, WORKER_JAR to the paths of the jar files you extracted.
![image](https://user-images.githubusercontent.com/101040591/205453522-0b1537ad-6ed4-4935-9fb9-f04f0852a614.png)

8.	Add 4 arguments to "LocalAppProject"
    a. Path to the input file (txt extension)
    b. Path to the output file (html extension)
    c. The amount of workers required to process the input file.
    d. (Optional) "-t" for termination 
    ![image](https://user-images.githubusercontent.com/101040591/205453532-4664c2b1-c983-4183-8375-41071e1491de.png)

 
# System Architecture
The system is composed of 3 elements:
 	o Local application
  o Manager
 	o Workers
The elements communicate with each other using queues (SQS) and storage (S3).
![image](https://user-images.githubusercontent.com/101040591/205453544-d28834db-946d-4de5-ab52-912a97a81b0e.png)

# Local -
The local is the manager's client, he is responsible for sending a new NewTask message to the Manager that contains the images to be processed. First, the Local validates the arguments he received, secondly, he initializes all the components relevant to AWS if they do not already exist, and then, sends an input message and waits for a reply from the Manager. Also, Local has a mechanism where it verifies that the Manager is still "alive" by sending pings. If the Manager does not respond after a certain number of pings sent to him, the Local creates a new connection request to a new Manager.

# Manager – 
The Manager is built similar to 'Reactor'. It is divided into several components:
'Postman' - responsible for pulling new messages from the SQS queue relevant to the Manager and classifying the messages according to the received type and sending the messages to a local queue defined in the Manager.
'LocalTaskHandler' - Inside the Manager there is a Thread pool whose task is to handle the messages coming from Locals, this thread receives new NewTask messages, reads the content of the message from S3, assigns Workers if necessary, and generates subtasks which it puts in the unqiue SQS of the Workers.
'WorkerTaskHandler' - Inside the Manager there is a Thread pool whose is to handle the messages coming from Workers, this thread receives a message from a Worker, saves its content for the relevant client and if all the tasks for a specific client have been processed, performs the merge operation and sends an output message to Local.
'PingService' - Thread whose job is to handle the ping message you receive for a specific client, receives the ping message, and sends a ping message back for that recipient.
![image](https://user-images.githubusercontent.com/101040591/205453550-bd637a26-c5d4-40cf-9ea1-dcc72e0544fb.png)

# Worker - 
The Worker has a main thread whose role is to pull a task from the queue, process it, create an output message for that task and send it back to the Manager.
Also, the Worker has another thread that is triggered every time a message is pulled from the queue whose role is to extend the Visibility Timeout for a specific task every 80 seconds. (more on "persistence")

# Scalability
o Each Local has a unique identifier, so that when a message is sent from a Local to a Manager, the Manager knows which Local sent him a message.
o Manager has a data structure where it stores all its active customers with their unique ID.
o Manager has a data structure where it stores the number of remaining tasks for each client.
o The Manager is built like a Reactor so that it supports receiving multiple messages from new clients, sending messages to the Worker, merging results, and sending an output message to the client in an efficient and parallel way (more in "System architecture").
o The Workers are created dynamically according to the number of active tasks with the Manager, so that the workforce is properly utilized, and can be increased according to the amount of work.
o Each Worker works on one task that he pulls from the unique queue for the Workers

# Persistence
o After sending the input message to the Manager, each local initiates sending a ping to the Manager every 20 seconds, when the Manager receives a Ping type message, it must return a ping to the person who sent it. If the client does not receive an answer for a certain number of pings it sent (the user can change this amount by changing the constant), the Local will conclude that the Manager "died" and is no longer responding and will reboot.
o Restarting Local including killing the previous Manager, creating a new Manager, and sending the input message to the new Manager.
o The Worker pulls a message from SQS common to all active Workers. Each message has a Visibility Timeout of 20 seconds, and also, in each Worker there is a mechanism that dynamically increases the Visibility Timeout for the message. In this way, even if a Worker "dies" after a short time, the message he was working on will become visible again and another Worker can pull it from the queue and work on it.

# Termination
o There is an option to add a fourth argument to the localAppProject so that    after performing the local tasks, a termination message is sent to the Manager.
o If the Manager received a Termination message, he stops pulling new messages from his SQS queue, and finishes the work on all the active tasks he has, after that, sends Termination messages to the active Workers and Terminate himself

# Effective use of cloud services
The project significantly saves on cloud services.
o Every new client ('LocalAppProject') works against the same server (Manager)
o Manager has one queue in the cloud from which he reads messages
o All Workers have one queue in the cloud from which they read messages
o The Manager regulates the amount of Workers required at any given time so that there is no less or excess amount of Workers for the total tasks the Manager has at a given time.
- Any message that is read from S3 and is no longer used, will be deleted directly after reading it.

# Security
o The credentials are not written hard coded anywhere in the project.
o The credentials are exchanged and saved each time inside the local computer under a folder that we protected with a password

# Running Times
For the following input we got the following running times:
 ![image](https://user-images.githubusercontent.com/101040591/205453559-5eaa4a80-5422-4ce5-9c85-9590ab87a84f.png)

* Most of the running time is for uploading the Jar to S3
![image](https://user-images.githubusercontent.com/101040591/205453564-00b189ad-fb2f-4e18-b301-7aaeda8b1707.png)

# Conclusions 
o When there are many Locals, there is a lot of load on the Manager, so you have to use a very strong Manager (not scalable) or alternatively, decentralize the Manager's work.
o Try other alternatives to OCR translation packages that can produce better performance.
