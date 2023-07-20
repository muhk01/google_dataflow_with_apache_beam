import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse
from datetime import datetime

parser = argparse.ArgumentParser() 
    
parser.add_argument('--input1',
                    dest='input1',
                    required=True,
                    help='Input file 1 to process.')

parser.add_argument('--input2',
                    dest='input2',
                    required=True,
                    help='Input file 2 to process.')

parser.add_argument('--output',
                      dest='output',
                      required=True,
                      help='Output file to write results to.')

path_args, pipeline_args = parser.parse_known_args()   

inputs1_pattern = path_args.input1  
inputs2_pattern = path_args.input2 

outputs_prefix = path_args.output 


options = PipelineOptions(pipeline_args)

def calculate_points(element):
    customer_id, first_name, last_name, relationship_id, card_type, max_limit, spent, cash_withdrawn, payment_cleared, payment_date = element.split(',')
    spent = int(spent)
    payment_cleared = int(payment_cleared)
    max_limit = int(max_limit)
    
    key_name = customer_id + ',' + first_name + ',' + last_name
    defaulter_points = 0
    
    if (payment_cleared > (spent*0.7)):
        defaulter_points += 1
    if (spent == max_limit) and (payment_cleared < spent):
        defaulter_points += 1
    if (spent == max_limit) and (payment_cleared < (spent*0.7)):
        defaulter_points += 1
    return key_name, defaulter_points

def calculate_month(input_list): #input --> [CT88330,Humberto,Banks,Serviceman,LN_1559,Medical Loan,26-01-2018, 2000, 30-01-2018]
    payment_date = datetime.strptime(input_list[8].rstrip().lstrip(), '%d-%m-%Y') #get array of index 8 (payment_date) convert to month
    input_list.append(str(payment_date.month)) #append month to last array [CT88330,Humberto,Banks,Serviceman,LN_1559,Medical Loan,26-01-2018, 2000, 30-01-2018, 1]
    
    return input_list
    
def calculate_late_payment(elements):
    due_date = datetime.strptime(elements[6].rstrip().lstrip(), '%d-%m-%Y')
    payment_date = datetime.strptime(elements[8].rstrip().lstrip(), '%d-%m-%Y')

    if payment_date <= due_date:
        elements.append('0')
    else:
        elements.append('1')
    return elements

def calculate_personal_loan_defaulter(input): #input -->  CT6855, Ronald Chiki value --> [01,05,06,07,08,09,10,11,12]
    max_allowed_missed_months = 4
    max_allowed_consecutive_missing = 2
    
    name, months_list = input                 #input [CT6855, Ronald, Chiki, Serviceman, LN_8460, Personal Loan, 25-01-2018, 50000, 25-01-2018]
    months_list.sort()
    sorted_months = months_list
    total_payments = len(sorted_months)
    
    missed_payments = 12 - total_payments
    
    if missed_payments > max_allowed_missed_months:
        return name, missed_payments
    
    consecutive_missed_months = 0
    
    temp = sorted_months[0] - 1
    if temp > consecutive_missed_months:
        consecutive_missed_months = temp
    
    temp = 12 - sorted_months[total_payments-1]
    if temp > consecutive_missed_months:
        consecutive_missed_months = temp
    
    for i in range(1, len(sorted_months)):
        temp = sorted_months[i] - sorted_months[i-1] -1
        if temp > consecutive_missed_months:
            consecutive_missed_months = temp
    
    if consecutive_missed_months > max_allowed_consecutive_missing:
        return name, consecutive_missed_months
    return name, 0
    
def format_output(sum_pair):
    key_name, miss_months = sum_pair
    return str(key_name) + ',' + str(miss_months) + ' missed'

def format_result(sum_pair):
    key_name, points = sum_pair
    return str(key_name) + ',' + str(points) + ' fraud_points'


def return_tuple(element):
    temp_tuple=element.split(',')
    return(temp_tuple[0], temp_tuple[1:])
    
with beam.Pipeline(options=options) as p:
    card_defaulter = (
        p
        | "read CC data" >> beam.io.ReadFromText(inputs1_pattern, skip_header_lines=1)
        | "calc default point" >> beam.Map(calculate_points)
        | "Sum default total" >> beam.CombinePerKey(sum)
        | "filter card defaulter" >> beam.Filter(lambda element: element[1] > 0)
        | "output" >> beam.Map(format_result)
        | "output on tuple" >> beam.Map(return_tuple)
        #| "output file" >> beam.io.WriteToText('./testdata/beam_data/bank/bank/output/default')
    )
    medical_loan_defaulter = (
        p
        | beam.io.ReadFromText(inputs2_pattern )
        | "split row" >> beam.Map(lambda row: row.split(','))
        | "filter medical" >> beam.Filter(lambda element: (element[5]).rstrip().lstrip() == 'Medical Loan')
        | "calculate late payment" >> beam.Map(calculate_late_payment)
        | "make key value pairs" >> beam.Map(lambda element: (element[0] + ', ' + element[1]+ ' ' + element[2],int(element[9]))) #id, first last name, number of missed
        | "group medical loan based on month" >> beam.CombinePerKey(sum)
        | "format medical loan output" >> beam.Map(format_output)
        #| "output file_med" >> beam.io.WriteToText('./testdata/beam_data/bank/bank/output/loanmed')
    )
    
    personal_loan_defaulter = (
        p
        | "read" >> beam.io.ReadFromText(inputs2_pattern )
        | "split" >> beam.Map(lambda row: row.split(','))
        | "filter personal" >> beam.Filter(lambda element: (element[5]).rstrip().lstrip() == 'Personal Loan')
        | "split and append new months" >> beam.Map(calculate_month)
        | "make key value pairs loan" >> beam.Map(lambda elements: (elements[0] + ', ' +elements[1] + ' ' +elements[2], int(elements[9])))
        | "group personal loan based on month" >> beam.GroupByKey()
        | "check for personal loan defaulter" >> beam.Map(calculate_personal_loan_defaulter)
        | "filter only defaulter" >> beam.Filter(lambda element:element[1]>0)
        | "format personal loan output" >> beam.Map(format_output)
        #| "output file_personal" >> beam.io.WriteToText('./testdata/beam_data/bank/bank/output/loanpersonal')
    )
    final_loan_defaulter = (
        (personal_loan_defaulter, medical_loan_defaulter)
        | "union both defaulter" >> beam.Flatten()
        #| "test" >> beam.Map(print)
        | "output tuple" >> beam.Map(return_tuple)
    )
    # join for card defaulter and flattened result of both loan defaulter
    both_defaulters = (
        {'card_defaulter':card_defaulter, 'loan_defaulter':final_loan_defaulter}
        | "Join" >> beam.CoGroupByKey()
        | "output file_personal" >> beam.io.WriteToText(outputs_prefix)
    )
    
