#!/bin/bash

# Prompt the user for for cde path
read -p "Enter cde PATH [./]: " CDE_PATH
CDE_EXEC=${CDE_PATH:-./}cde
if [ -f "$CDE_EXEC" ]; then
    echo "$CDE_EXEC exists."
    $CDE_EXEC --version
else 
    echo "$CDE_EXEC does not exist."
    exit 1
fi

# Prompt the user for the command to run
echo "Enter the username for clean up (or total number of users):"
read input_value

# Check if the input is empty (null)
if [ -z "$input_value" ]; then
    echo "Input is empty. Aborting the script."
    exit 1
fi

# Check if the input is numeric
if [[ "$input_value" =~ ^[0-9]+$ ]]; then
    users=()
    for ((i = 1; i <= input_value; i++)); do
        users+=($(printf "user%03d" "$i"))
    done
else
    users=($input_value)
fi

echo "Generated users list: ${users[*]}"

# Command to run for each user
command_to_run="$CDE_EXEC job delete --name "

job_list=(
'01_Setup'
'02_EnrichData_ETL'
'03_Spark2Iceberg'
'04_Sales_Report'
'05_A_ETL'
'05_B_Reports'
'06_pyspark_sql'
'07_A_pyspark_LEFT'
'07_B_pyspark_RIGHT'
'07_C_pyspark_JOIN'
'08_etl_job'
'08_gen_table'
)

# Iterate through the list of users
for user in "${users[@]}"; do
    # Execute the command for each user
    for job in "${job_list[@]}"; do
        echo "Delete job name: ${user}_${job} :"
        $command_to_run ${user}_${job}
        if [ $? -eq 0 ]; then
            echo -e "\tDELETED: ${user}_${job}"
        fi
    done
done
