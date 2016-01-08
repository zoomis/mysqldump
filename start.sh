#/bin/sh -f
# Loading parameters
source dump.params
OUTPUT_DIR=output-$(hostname)
NL=`echo -ne '\015'`

# Check to see if we are already running DevStack                               
# Note that this may fail if USE_SCREEN=False                                   
if type -p screen > /dev/null && screen -ls | egrep -q "[0-9]\.$SCREEN_NAME";
then
    echo "You are already running a stack.sh session."                          
    echo "To rejoin this session type 'screen -x $SCREEN_NAME'."                       
    echo "To destroy this session, type './stop.sh'."                        
    exit 1                                                                      
fi
                 

#execute command on screen
screen -d -m -S $SCREEN_NAME -t shell -s /bin/bash
sleep 1
# Set a reasonable status bar                                               
SCREEN_HARDSTATUS='%{= .} %-Lw%{= .}%> %n%f %t*%{=
.}%+Lw%< %-          =%{g}(%{d}%H/%l%{g})'
screen -r $SCREEN_NAME -X hardstatus alwayslastline "$SCREEN_HARDSTATUS"    
screen -r $SCREEN_NAME -X setenv PROMPT_COMMAND /bin/true                   
                     
# run process
#screen -S $SCREEN_NAME -X screen -t monitor
#screen -S $SCREEN_NAME -p monitor -X stuff "watch -n $FREQUENCY 'ls -l $OUTPUT_DIR | grep -c \^d | wc -l' $NL"
screen -S $SCREEN_NAME -X screen -t dump
screen -S $SCREEN_NAME -p dump -X stuff "bash dump_all.sh $NL"
