#/bin/sh -f
# Loading parameters
source dump.params
OUTPUT_DIR=output-$(hostname)
python mysqldump_all.py $DB_USER $DB_PASS $DB_HOST $FREQUENCY $DURATION $OUTPUT_DIR $DATABASES $EXCLUDED_TABLES $LOG_SERVER $LOG_USER $KEY_FILE $TARGET
