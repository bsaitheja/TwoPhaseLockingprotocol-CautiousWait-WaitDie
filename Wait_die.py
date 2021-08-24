from tabulate import tabulate
import sys
import os.path
import pandas as callit
import numpy as np
from art import *



tprint("Welcome  to  two  phase  lock")
global fname
fname=input(" Please enter the file name with .txt: (example : \"input.txt\") :")
output=input("For Tabular output enter 1 or for short summary of the each operation press any other key : ")


def transactionTable():
    columns = ['id','ts_var','state','items','data_in_file']
    return callit.DataFrame(columns = columns)
    
def writeFile():
    try:
        if(output=='1'):
            f = open("header.txt")
            f1 = open(fname+"'s output - waitDie - Tabular Format", "w+")
            for line in f:
                f1.write(line)
        else:
            f = open("header.txt")
            f1 = open(fname+"'s output - waitDie - Summary Format", "w+")
            for line in f:
                f1.write(line)
        f1.write("\n")
        f1.write("\n")
        f1.write("Wait Die")
        f1.write("\n")
        f1.write("\n")
        return f1
    except:
        print("Invalid filename please re-run the code and select valid filename")

def ck_frame():
    columns = ['item_name','state','tid_hold','tid_wait']
    return callit.DataFrame(columns = columns)
    

def write():
    try:
        global tran_fun
        global lock_fun
        global file_writer
        file_writer.write("\n")
        file_writer.write("Transaction Table:")
        file_writer.write("\n")
        file_writer.write(tabulate(tran_fun,headers='keys', tablefmt='psql'))
        file_writer.write("\n")
        file_writer.write("\n")
        file_writer.write("Lock Table:")
        file_writer.write("\n")
        file_writer.write(tabulate(lock_fun,headers='keys', tablefmt='psql'))
        file_writer.write("\n")
        file_writer.write("\n")
        file_writer.write("*********************************************************************************************")
        file_writer.write("\n")
        file_writer.write("\n")
    except:
        print("Please re-run the code, something unexpected happend :(")

def write1():
    try:
        global file_writer
        file_writer.write("\n")
        file_writer.write("---------------------------------------------------------------------------------------------")
        file_writer.write("\n")
    except:
        print("Please re-run the code, something unexpected happend :(")


#In this function, we getting the input from main() function (data from the text file as operation) and timestamp and check for the possible operation like b (begin transaction), r (read item), w (write item), e (end transaction), c (commit transaction) and call appropriate functions.
def execute(data,ts_var):
    try:
        global tran_fun
        global lock_fun
        global file_writer

        
        ongoing_process = 'T' + data[1]

    
        if data[0] != 'b' and data[0] != 'c' and data[0] != 'e': 
            operation_value = data[3]

    
        if data[0] == 'b':
            begin_transaction(ongoing_process, ts_var, data)
        elif data[0] == 'r':
            operation_value = data[3]
            read_lock(operation_value, ongoing_process, data)
        elif data[0] == 'w':
            operation_value = data[3]
            lock_write(operation_value, ongoing_process, data)
        elif data[0] == 'c' or data[0] == 'e':
            commit(ongoing_process, data)
    except:
        print("Please re-run the code, something unexpected happend :(")


#In this function the operation is check read/write/commit/about and calls the respective function
def begin_transaction(ongoing_process, ts_var, data):
    try:
        global tran_fun
        global lock_fun
        global file_writer
        data = data.replace('\n','')
        tran_fun = tran_fun.append(callit.Series([ongoing_process, ts_var, 'Active', '',''], index=tran_fun.columns ), ignore_index=True)
        file_writer.write(data + ' Begin ' + ongoing_process + ":Record is added to transaction table with Tid="+data[1]+"\n \n")
    except:
        print("Please re-run the code, something unexpected happend :(")
    
#In this function, we collect all the locks created by the transaction in list and unlock them And update the transaction status as aborted.
def abort(ongoing_process, data):
    try:
        global tran_fun
        global lock_fun
        global file_writer

        sealed_items = []

        process_list = tran_fun.loc[tran_fun['id'] == ongoing_process,['id','ts_var','state','items','data_in_file']] 

        items_sealed = process_list['items'].to_list()[0]
        sealed_items = items_sealed.split('-')

        tran_fun.loc[tran_fun['id'] == ongoing_process, ['state']] = 'Aborted'
        data = data.strip()
        file_writer.write(data + 'Transaction ' + ongoing_process + ' aborted \n')
        
        for data_in_lis in sealed_items:
            if data_in_lis.strip() != '':
                unlock(ongoing_process, data_in_lis)
    except:
        print("Please re-run the code, something unexpected happend :(")


# In this function, we check if the items are present in lock table if not then we add item to lock table with lock state as write. And if the item has write lock then wait die or cautious waiting process is called. If the item has read lock then update the item in lock table with corresponding transaction id from read to write.
def lock_write(operation_value, ongoing_process, data):
    try:
        global tran_fun
        global lock_fun
        global file_writer
        
        see_ms = lock_fun.loc[lock_fun['item_name'] == operation_value,['item_name','state','tid_hold','tid_wait']] 
        
        ld_es = see_ms['tid_hold'].to_list()[0]
        holding_tList = ld_es.split('-')
        
        a_es = see_ms['tid_wait'].to_list()[0]
        it_ui = a_es.split('-')
        
        present_tem = see_ms
        cs_ed = present_tem['tid_hold'].to_list()[0]

        cs_ed = cs_ed.replace('-', '')
    except:
        print("Please re-run the code, something unexpected happend :(")
    
    try:
        if (present_tem['state'].to_list()[0] == 'readlock') and (ongoing_process == cs_ed):
        
            lock_fun.loc[lock_fun['item_name'] == operation_value, ['state']] = 'writelock'
            data = data.strip()
            file_writer.write( data + ' Read lock upgraded to write lock on ' + operation_value + ' by transaction ' + ongoing_process+'\n')
            
            
        else:
    
            if present_tem['state'].to_list()[0] == 'Unlocked':
                din_ion1 = ''
                for each_data_in_list in holding_tList:
                    if each_data_in_list != '':
                        din_ion1 = din_ion1 + each_data_in_list + '-'
                din_ion1 = din_ion1 + ongoing_process + '-'
                lock_fun.loc[lock_fun['item_name'] == operation_value, ['tid_hold']] = din_ion1
                lock_fun.loc[lock_fun['item_name'] == operation_value, ['state']] = 'writelock'
                
    
                process_list = tran_fun.loc[tran_fun['id'] == ongoing_process,['id','ts_var','state','items','data_in_file']] 

                rnt_ctm = process_list['items'].to_list()[0]
                as_et = rnt_ctm.split('-')
                
                tm_ae = ''
                as_et.append(operation_value)
                for each_data_in_list in as_et:
                    if each_data_in_list != '':
                        tm_ae = tm_ae + each_data_in_list + '-'
                tran_fun.loc[tran_fun['id'] == ongoing_process, ['items']] = tm_ae
                data = data.strip()
                file_writer.write( data + ' Write lock acquired on ' + operation_value + " by transaction " + ongoing_process +"\n")
                

                
            else:
                
                ol_in_is = []
            
                for each_data_inlist in holding_tList:
                    if each_data_inlist != '' and each_data_inlist != ongoing_process:
                        ol_in_is.append(each_data_inlist)
                

                if len(ol_in_is) == 1:
                    waitdie(ol_in_is[0], ongoing_process, operation_value, data)
                else:
                    process_list = tran_fun.loc[tran_fun['id'] == ongoing_process,['id','ts_var','state','items','data_in_file']] 
                    
                    rnt_est = process_list['ts_var'].to_list()[0]
                    toogle = 0
                    if len(ol_in_is) > 0:
                        for capture in ol_in_is:
                            process_list = tran_fun.loc[tran_fun['id'] == capture,['id','ts_var','state','items','data_in_file']] 						

                            cvb_stm = process_list['ts_var'].to_list()[0]
            
                            if rnt_est > cvb_stm: toogle = 1
                            else:	
                                toogle = 0
                                break
                        if toogle == 0:
                            file_writer.write(data+" : "+ongoing_process + ' Transaction will wait \n')
                            
                            tran_fun.loc[tran_fun['id'] == ongoing_process, ['state']] = 'Blocked'
                            tran_fun.loc[tran_fun['id'] == ongoing_process, ['data_in_file']] = data

                            hold_trann = ''
                            for each_data_in_list in it_ui:
                                if each_data_in_list != '':
                                    hold_trann = hold_trann + each_data_in_list + '-'

                        
                            lock_fun.loc[lock_fun['item_name'] == operation_value, ['tid_wait']] = hold_trann
                        else:
                            abort(ongoing_process,data)
    except:
        print("Please re-run the code, something unexpected happend :(")

#we take timestamp of transaction which has lock and timestamp of transaction requesting for the operation and check which is greater, if timestamp of operation is greater then abort the transaction otherwise the transaction will wait.
def waitdie(onhold_tran, requesting_transactions, gg_em, data):
    try:
        global tran_fun    
        global lock_fun
        global file_writer

        process_list = tran_fun.loc[tran_fun['id'] == onhold_tran,['id','ts_var','state','items','data_in_file']] 		
        HTS = process_list['ts_var'].to_list()[0]

        process_list = tran_fun.loc[tran_fun['id'] == requesting_transactions,['id','ts_var','state','items','data_in_file']] 		
        RTS = process_list['ts_var'].to_list()[0]
        if RTS < HTS:

            file_writer.write(requesting_transactions + ' Transaction will wait \n')
            
            tran_fun.loc[tran_fun['id'] == requesting_transactions, ['state']] = 'Blocked'
            tran_fun.loc[tran_fun['id'] == requesting_transactions, ['data_in_file']] = data			
            
            seal_list_var = lock_fun.loc[lock_fun['item_name'] == gg_em,['item_name','state','tid_hold','tid_wait']]
            
            hold_tran = seal_list_var['tid_wait'].to_list()[0]
            hold_tran_list = hold_tran.split('-')

            onhold_tran = seal_list_var['tid_hold'].to_list()[0]
            onhold_tran_list = onhold_tran.split('-')


            hold_trann = ''
            
            for datainlist in hold_tran_list:
                if datainlist != '':
                    hold_trann = hold_trann + datainlist + '-'
            hold_trann = hold_trann + requesting_transactions + '-'
            lock_fun.loc[lock_fun['item_name'] == gg_em, ['tid_wait']] = hold_trann

            
        else:
            file_writer.write(data+': As per wait die methodology , '+ requesting_transactions + ' transaction will abort as it is younger than transaction ' + onhold_tran+'\n')
            abort(requesting_transactions, data)
    except:
        print("Please re-run the code, something unexpected happend :(")

#In this function, we check if the items are present in lock table if not then we add item to lock table with lock state as read. And if the item has write lock then wait die or cautious waiting is called, otherwise update the item in lock table with corresponding transaction id.
def read_lock(operation_value, ongoing_process, data):
    global tran_fun
    global lock_fun
    global file_writer
    
    see_ms = lock_fun.loc[lock_fun['item_name'] == operation_value,['item_name','state','tid_hold','tid_wait']] 
    try:
        if see_ms.size > 0:        
            yi_og = see_ms['tid_hold'].to_list()[0]
            yi_og_it = yi_og.split('-')
            gg_em = see_ms
    except:
        print("Please re-run the code, something unexpected happend :(")
    
    try:
        if see_ms.size == 0:
            lock_fun = lock_fun.append(callit.Series([operation_value, 'readlock', ongoing_process+'-', ''], index=lock_fun.columns ), ignore_index=True) 
            process_list = tran_fun.loc[tran_fun['id'] == ongoing_process,['id','ts_var','state','items','data_in_file']] 
            fy_oj = process_list['items'].to_list()[0]
            fy_oj_em = fy_oj.split('-')
            fy_oj = ''

            for present_tem in fy_oj_em:
                if present_tem != '':
                    fy_oj = fy_oj + present_tem + '-'
            fy_oj = fy_oj + operation_value + '-'
            
            tran_fun.loc[tran_fun['id'] == ongoing_process, ['items']] = fy_oj
            data = data.strip()
            file_writer.write( data + ' Read lock acquired on ' + operation_value + ' by '+ ongoing_process+'\n')
            
        else:
            
            if gg_em['state'].to_list()[0] != 'writelock':
                yi_og_it.append(ongoing_process)
                os_ld = ''
                for each_data_in_list in yi_og_it:
                    if each_data_in_list != '':
                        os_ld = os_ld + each_data_in_list + '-'

                lock_fun.loc[lock_fun['item_name'] == operation_value, ['tid_hold']] = os_ld
                lock_fun.loc[lock_fun['item_name'] == operation_value, ['state']] = 'readlock'
                
                process_list = tran_fun.loc[tran_fun['id'] == ongoing_process] 
            
                rnt_ctm = process_list['items'].to_list()[0]

                nct_rs = rnt_ctm.split('-')
                
                tm_ae = ''
                nct_rs.append(operation_value)
                for each_data_inlist in nct_rs:
                    if each_data_inlist != '':
                        tm_ae = tm_ae + each_data_inlist + '-'

                tran_fun.loc[tran_fun['id'] == ongoing_process, ['items']] = tm_ae
                data = data.replace('\n','')
                file_writer.write( data + ' Read lock granted on ' + operation_value + ' by'+ ongoing_process+'\n')
                
            else:
                onhold_tran = yi_og_it[0]
                requesting_transactions = ongoing_process
                waitdie(onhold_tran, requesting_transactions, operation_value, data)
    except:
        print("Please re-run the code, something unexpected happend :(")

#we collect all the locks created by the transaction in list and unlock them then commit the transaction.
def  commit(ongoing_process, data):
    try:
        global tran_fun
        global lock_fun
        global file_writer

        sealed_items = []
        
        process_list = tran_fun.loc[tran_fun['id'] == ongoing_process,['id','ts_var','state','items','data_in_file']] 

        items_sealed = process_list['items'].to_list()[0]
        sealed_items = items_sealed.split('-')

        
        tran_fun.loc[tran_fun['id'] == ongoing_process, ['state']] = 'commited'
        tran_fun.loc[tran_fun['id'] == ongoing_process, ['items']] = ' '
        data = data.strip()
        file_writer.write(data + ' Transaction ' + ongoing_process + ', state = committed. Releases all locks held by '+ ongoing_process+'\n')
        
        for data_in_lis in sealed_items:
            if data_in_lis.strip() != '':
                unlock(ongoing_process, data_in_lis)
    except:
        print("Please re-run the code, something unexpected happend :(")
    
#We collect all the list of waiting transactions and check if the transaction is not in waiting then update the lock table for the corresponding item and change state to Unlock
def unlock(ongoing_process, operation_value):
    try:
        global tran_fun
        global lock_fun
        global file_writer

        seal_list_var = lock_fun.loc[lock_fun['item_name'] == operation_value,['item_name','state','tid_hold','tid_wait']] 
        hold_tran = seal_list_var['tid_wait'].to_list()[0]
        hold_tran_list = hold_tran.split('-')
        onhold_tran = seal_list_var['tid_hold'].to_list()[0]
        onhold_tran_list = onhold_tran.split('-')
        onhold_tran = ''	
        terminal_onhold_list = []
        for each_data_in_list in onhold_tran_list:
            if each_data_in_list != '' and each_data_in_list != ongoing_process:
                onhold_tran = onhold_tran + each_data_in_list + '-'
                terminal_onhold_list.append(each_data_in_list)
        
        if len(terminal_onhold_list) == 0:
            lock_fun.loc[lock_fun['item_name'] == operation_value, ['state']] = 'Unlocked'
            lock_fun.loc[lock_fun['item_name'] == operation_value, ['tid_hold']] = onhold_tran

        else:
            
            lock_fun.loc[lock_fun['item_name'] == operation_value, ['tid_hold']] = onhold_tran
            
        terminal_hold_tran_list = []
        for each_data_inlist in hold_tran_list:
            if each_data_inlist != '':
                terminal_hold_tran_list.append(each_data_inlist)

        if len(terminal_hold_tran_list) != 0:

            current_waiting_t = terminal_hold_tran_list[0]
            terminal_hold_tran_list.remove(current_waiting_t)

            revice_tran = ''
            revice_tran = ''.join(revice_tran + each_data_inlist + '-' for each_data_inlist in terminal_hold_tran_list)
            
            lock_fun.loc[lock_fun['item_name'] == operation_value, ['tid_wait']] = revice_tran

            file_writer.write( current_waiting_t + ' has resumed its operation \n')
            
            tran_fun.loc[tran_fun['id'] == current_waiting_t, ['state']] = 'Active'
            
            process_list = tran_fun.loc[tran_fun['id'] == current_waiting_t,['id','ts_var','state','items','data_in_file']] 

            commiting_inst = process_list['data_in_file'].to_list()[0]
            commiting_list = commiting_inst.split(';')
            commiting_inst_end = []
            for each_data_in_list in commiting_list:
                if each_data_in_list != '':
                    each_data_inlist = each_data_in_list.replace('\n','')
                    terminal_hold_tran_list.append(each_data_inlist)
            for data in commiting_list:
                data = data.replace('\n', '')
                if data != '':
                    execute(data, 1)
                    latest_ope = ''
                    for data_in_li in commiting_inst_end:
                        if data_in_li != data:
                            latest_ope = latest_ope + data_in_li
                    tran_fun.loc[tran_fun['id'] == ongoing_process, ['data_in_file']] = latest_ope

    except:
        print("Please re-run the code, something unexpected happend :(")



#In this function, we are assigning Timestamp to 0 and read a text file, passed from the system arguments as an input and get the transaction id form it. And we are checking the transaction state, if it is in “Aborted state” then it will discard the transaction, or if it “Blocked” then add to LockTable, otherwise send the data of text file to execute function along with the timestamp
def main():
    global tran_fun
    global lock_fun
    global file_writer
    try:
        with open(fname,'r') as data_in_file: 
            ts_var = 0
            for data in data_in_file.readlines():
                if(output=='1'):
                    write()
                else:
                    write1()
                data = data.replace(' ', '')
                ongoing_process = 'T' + data[1]
                tran_record = tran_fun.loc[tran_fun['id'] == ongoing_process,['id','ts_var','state','items','data_in_file']] 
                
                if tran_record.size == 0:
                    ts_var = ts_var + 1 if data[0] == 'b' else ts_var
                    execute(data, ts_var)
                else:
                    if tran_record['state'].to_list()[0] == 'Aborted':
                        file_writer.write(data+' : Operation not executed as Transaction ' + ongoing_process + ' is aborted  ignored\n')
                        
                    else:

                        if tran_record['state'].to_list()[0] == 'Blocked':
                            ope_interrupted = tran_record['data_in_file']
                            data = data.replace('\n','')
                            ope_interrupted = ope_interrupted.replace('\n','')
                            ope_interrupted = ope_interrupted + data
                            tran_fun.loc[tran_fun['id'] == ongoing_process, ['data_in_file']] = ope_interrupted
                            data = data.strip()
                            file_writer.write( data + ' Added to waiting data_in_file list for Transaction ' + ongoing_process+'\n')
                        else:
                            execute(data,ts_var)
        if(output=='1'):
            write()
        else:
            write1()
        print("Output file create for "+fname)
    except:
        print("Sorry Invalid file name, please enter correct filename ")
    
        

tran_fun = transactionTable()
lock_fun = ck_frame()
file_writer=writeFile()

if __name__ == '__main__':
    main()
