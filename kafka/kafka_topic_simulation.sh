#!/usr/bin/env bash
#
# This bash script will accomplish the following tasks:
# 
#  - create  (N) topics
#  - produce (N) messages in each topic
#  - consume (N) topics
#  - delete  (N) newly created topics
#  - enable producer parallelism 
#
#  NOTE.
# 
#   The following script call will:
#     - create "2" topic
#     - produce "10" messages per topic -- parallelism of 20 producer at a time
#     - consume messages for 5 seconds, and
#     - delete newly "2" created topics
#
#   Usage: ./kafka_topic_simulation.sh <num_topics> <messages_per_topic> <consume_duration_sec> <parallel_workers>
#   Example: ./kafka_topic_simulation.sh 2 10 5 20
# 

log_message(){
    local message="$1"
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $message" >> ${LOG_FILE}
}

check_command_status(){
    local status=$?
    if [ $status -ne 0 ]; then
        log_message "====> Error: Last command failed with status $status"
        exit 1
    fi
}

create_topics(){
    log_message "====> Creating topics..."
    for i in $(seq 1 "$1"); do
        topic="${TOPIC_NAME}${i}"
        kafka-topics \
            --bootstrap-server $BROKER_LIST \
            --command-config $PROPERTIES_FILE \
            --create  --replication-factor 3 \
            --topic $topic > /dev/null 2>&1 &

        check_command_status
    done
    wait # Wait for any remaining jobs to finish
    log_message "====> Total Topics created: $i"

}

produce_messages(){
    local max_parallel=$4
    local current_jobs=0
    log_message "====> Producing messages..."
    for i in $(seq 1 "$1"); do
        topic="${TOPIC_NAME}${i}"
        for j in $(seq 1 "$2"); do
            {  
                echo "automated message $j for topic $topic " | \
                    kafka-console-producer \
                        --broker-list $BROKER_LIST  \
                        --producer.config=$PROPERTIES_FILE \
                        --topic $topic > /dev/null 2>&1
                
                check_command_status
            } &

            ((current_jobs++))

            if [[ "$current_jobs" -ge "$max_parallel" ]]; then
                wait -n  # Wait for one of the background jobs to finish
                ((current_jobs--))
            fi
        done
        log_message "====> Total messages produced per topic: $j"
    done
    wait # Wait for any remaining jobs to finish
    log_message "====> All messages produced."
}

consume_messages(){
    log_message "====> Consuming messages..."
    for i in $(seq 1 "$1"); do
        topic="${TOPIC_NAME}${i}"
        log_message "====> Consuming messages from Kafka topic: $topic for $CONSUMER_DURATION seconds"
        kafka-console-consumer \
            --bootstrap-server $BROKER_LIST \
            --consumer.config $PROPERTIES_FILE \
            --topic $topic \
            --from-beginning \
            --timeout-ms $((CONSUMER_DURATION  * 1000)) \
            --max-messages 1000 > /dev/null 2>&1 &

        check_command_status
    done
    wait # Wait for any remaining jobs to finish
}

delete_topics(){
    log_message "====> Deleting topics if exists..."
    for i in $(seq 1 "$1"); do
        topic="${TOPIC_NAME}${i}"
        kafka-topics \
            --bootstrap-server $BROKER_LIST  \
            --command-config $PROPERTIES_FILE \
            --delete \
            --topic $topic > /dev/null 2>&1 &

        check_command_status
    done
    wait # Wait for any remaining jobs to finish
    log_message "====> Total topics successful deleted: $i"

}

describe_topics(){
    log_message "====> Describe topics..."
    for i in $(seq 1 "$1"); do
        topic="${TOPIC_NAME}${i}"
        kafka-topics \
            --bootstrap-server $BROKER_LIST \
            --command-config $PROPERTIES_FILE \
            --describe \
            --topic $topic > /dev/null 2>&1 &

        check_command_status
    done
    wait # Wait for any remaining jobs to finish
}

main(){

    # Validate input arguments
    if [ $# -lt 4 ]; then
        echo "❌ Error: Missing arguments."
        echo "Usage: $0 <num_topics> <messages_per_topic> <consume_duration_sec> <parallel_workers>"
        echo "Example: $0 2 10 15 20"
        exit 1
    fi

    N_TOPICS="$1"
    N_PRODUCER="$2"
    CONSUMER_DURATION="$3"
    MAX_PARALLEL="$4"
    BROKER_LIST="`hostname -f`:9095"
    PROPERTIES_FILE="/root/producer.properties"
    TOPIC_NAME="my_topic_"
    LOG_FILE=kafka_topic_simulation.log
    
    log_message "DEBUG: Parameters received: N_TOPICS=$N_TOPICS, N_PRODUCER=$N_PRODUCER, CONSUMER_DURATION=$CONSUMER_DURATION, MAX_PARALLEL=$MAX_PARALLEL"
    log_message "====>Starting Kafka topic simulation..."
    
    delete_topics "${N_TOPICS}"
    create_topics "${N_TOPICS}"
    #describe_topics "${N_TOPICS}"
    produce_messages "${N_TOPICS}" "${N_PRODUCER}"
    consume_messages "${N_TOPICS}" "${CONSUMER_DURATION}"
    delete_topics "${N_TOPICS}"

    log_message "====> Kafka topic simulation completed."

}

main "$@"
