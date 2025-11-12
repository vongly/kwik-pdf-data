# pouch/envelope is sometimes blank
# fill in as 'blank'
def parse_columns(word_list, departments):
    parsed_words = []
    word_indices_used = []

    for i, word in enumerate(word_list):
        if i == 0:
            j = 0
        
        if j in [0, 1]:
            # 0 -> plu_number
            # 1 -> pkg_qty
            parsed_words.append(word)
            word_indices_used.append(i)
            j = j + 1
        elif j == 2:
            # 2 -> skips words to look for % of total
            # there are 4 requirements
            # --> '%' in word_list[i]
            # --> '%' in word_list[i-1]
            # --> '$' in word_list[i-2]
            # --> '$' in word_list[i-3]
            if '%' in word and '%' in word_list[i-1] and '$' in word_list[i-2] and '$' in word_list[i-3]:
                # appends % of total
                parsed_words.append(word)
                word_indices_used.append(i)
                j = j + 1

                # 3 -> appends the previous index as % of dept
                parsed_words.append(word_list[i-1])
                word_indices_used.append(i-1)
                j = j + 1

                # 4 -> appends the previous index as sales
                parsed_words.append(word_list[i-2])
                word_indices_used.append(i-2)
                j = j + 1

                # 5 -> appends the previous index as price
                parsed_words.append(word_list[i-3])
                word_indices_used.append(i-3)
                j = j + 1

                # 6 -> appends the previous index as count
                parsed_words.append(word_list[i-4])
                word_indices_used.append(i-4)
                j = j + 1

                # 7 -> checks previous word iterations for combinations that match w/ a department
                # looks back k words from count
                count_i = i-4
                # initial value = 1 word back
                k = -1
                while True:

                    dep_word_range = range(count_i+k, count_i)
                    
                    possible_department = []
                    for dep_i in dep_word_range:
                        possible_department.append(word_list[dep_i])

                    possible_department = ' '.join(possible_department)

                    word_indices_used.append(count_i+k)

                    # Handles description words that get attached to tender_type in parsing
                    add_on_to_description = ''
                    for dep in departments:
                        if dep in possible_department:
                            add_on_to_description = possible_department.replace(dep,'')
                            possible_department = dep

                    if possible_department in departments:
                        # MATCH -> move to the next column
                        parsed_words.append(possible_department)
                        j = j + 1
                        break
                    elif k < -10:
                        exit()
                    else:
                        # NO MATCH -> add the previous word into the phrase and check
                        k = k - 1 

                # 8 -> use the prior unused indices to build tender_type

                word_indices_used_full_range = set(range(min(word_indices_used), max(word_indices_used) + 1))
                unused_indices = sorted(word_indices_used_full_range - set(word_indices_used))

                description = ' '.join([word_list[index] for index in unused_indices]) + ' ' + add_on_to_description
                parsed_words.append(description)
                word_indices_used.extend(unused_indices)
                j = 0
            else:
                continue

    return parsed_words