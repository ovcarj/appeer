# pylint: skip-file

# Example script to print a PUBLISHER_journals.json file
# The list of journals should be given in PUBLISHER_journals_list.txt
# with each journal written in a new line

with open('PUBLISHER_journals_list.txt', 'r') as f:
    lines = f.readlines()

res = '{\n'

for i, line in enumerate(lines):

    line = line.rstrip('\n')

    res += '\n'

    res += f'\t"PUBLISHER_{i}":' + '\n\n\t\t{\n\n' + '\t\t"normalized_name":\t' + f'"{line}"' + ',\n'

    res += '\t\t"name_variants":\t[\n'

    res += f'\t\t\t\t\t"{line}"\n'

    res += '\t\t\t\t\t]\n'

    res += '\t\t},\n'

res = res[0:-2]

res += '\n\n}'

print(res)
