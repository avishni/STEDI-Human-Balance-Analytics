import os
import json

def refrmat_json(filename):
    with open(filename, 'r') as input_file:
        json_data = input_file.read()
        
    data = json_data.split('}')
    
    output_file = filename.replace('extracted', 'processed')
    with open(output_file, 'w') as output_file:
        for i in range(len(data)):
            # Append '}' to the end of each record
            data[i] += '}'
            try:
                # Load json data and dump it to output file
                json_data = json.loads(data[i])
                json.dump(json_data, output_file)
                output_file.write('\n')
            except json.decoder.JSONDecodeError:
                # in case of invalid json
                pass
    

def main(path):
    print ("Starting...")
    
    list_of_files = sorted( filter( lambda x: os.path.isfile(os.path.join(path, x)), os.listdir(path) ) )
    
    for filename in list_of_files:
        f = os.path.join(path, filename)
        if os.path.isfile(f):
            print(f)
            refrmat_json(f)
   
if __name__ == "__main__":
    path= "/home/cloudshell-user/extracted/step_trainer"
    main(path)
