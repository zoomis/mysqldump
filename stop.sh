#/bin/sh -f
SCREEN=$(which screen)                                                          
if [[ -n "$SCREEN" ]]; then                                                     
    SESSION=$(screen -ls | awk '/[0-9].mysqldump/ { print $1 }')                    
    if [[ -n "$SESSION" ]]; then                                                
        screen -X -S $SESSION quit                                              
    fi
                        fi                                                                              
                               
