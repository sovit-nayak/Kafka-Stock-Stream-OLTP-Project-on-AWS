
# Kafka Stock Stream OLTP Project on AWS

This project demonstrates a basic setup for Apache Kafka, including producers and consumers, to handle stock market data.

![Architecture](Architecture.jpg)

## Prerequisites

- Java 1.8.0
- Apache Kafka 3.3.1
- An EC2 instance (for deployment)

## Installation

### Step 1: Download and Extract Kafka

```bash
wget https://downloads.apache.org/kafka/3.3.1/kafka_2.12-3.3.1.tgz
tar -xvf kafka_2.12-3.3.1.tgz
```

### Step 2: Install Java

Check if Java is installed:

```bash
java -version
```

If not installed, use:

```bash
sudo yum install java-1.8.0-openjdk
java -version
```

### Step 3: Start ZooKeeper

Navigate to the Kafka directory and start ZooKeeper:

```bash
cd kafka_2.12-3.3.1
bin/zookeeper-server-start.sh config/zookeeper.properties
```

### Step 4: Start Kafka Server

Open a new terminal, SSH into your EC2 instance, and start the Kafka server:

```bash
export KAFKA_HEAP_OPTS="-Xmx256M -Xms128M"
cd kafka_2.12-3.3.1
bin/kafka-server-start.sh config/server.properties
```

### Step 5: Configure Kafka for Public Access

Edit the `server.properties` file to set the public IP of your EC2 instance:

```bash
sudo nano config/server.properties
# Change the ADVERTISED_LISTENERS property to your public IP
```

## Creating and Using Topics

### Create a Topic

Open a new terminal and create a topic named `demo_test`:

```bash
cd kafka_2.12-3.3.1
bin/kafka-topics.sh --create --topic demo_test --bootstrap-server <PUBLIC_IP>:9092 --replication-factor 1 --partitions 1
```

### Start a Producer

Start a Kafka producer to send messages to the `demo_test` topic:

```bash
bin/kafka-console-producer.sh --topic demo_test --bootstrap-server <PUBLIC_IP>:9092
```

### Start a Consumer

Open a new terminal and start a Kafka consumer to read messages from the `demo_test` topic:

```bash
cd kafka_2.12-3.3.1
bin/kafka-console-consumer.sh --topic demo_test --bootstrap-server <PUBLIC_IP>:9092
```

## Jupyter Notebooks

The project includes Jupyter notebooks for producing and consuming stock market data using Kafka.

- `kafka_producer.ipynb`: Notebook for Kafka producer implementation.
- `kafka_consumer.ipynb`: Notebook for Kafka consumer implementation.

## Security

- `kafka-stock-market-project.pem`: PEM file for securing your EC2 instance.

## Data

- `indexProcessed.csv`: CSV file containing processed stock market data.

## Running the Notebooks

Ensure you have Jupyter installed. To run the notebooks, execute the following command in the terminal:

```bash
jupyter notebook
```

Then open `kafka_producer.ipynb` and `kafka_consumer.ipynb` to interact with the Kafka setup.

## License

This project is licensed under the MIT License.
