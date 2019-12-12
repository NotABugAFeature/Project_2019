# Ανάπτυξη Λογισμικού για Πληροφοριακά Συστήματα
## Εφαρμογή: Project_2019_2
## Αρχείο: README.md  
### Ομάδα
Γεωργακόπουλος Παναγιώτης 1115201600028\
Καραμηνά Μαρία            1115201600059\
Κουρσιούνης Γεώργιος      1115201600077
- - - -

### 1) Λίγα λόγια για την υλοποίηση της εργασίας ανά αρχείο
Η υλοποίηση της εργασίας έγινε σε γλώσσα C (C11) και αποτελείται από δώδεκα (12) αρχεία:

#### script.sh
τροφοδοτεί τη main με τους πίνακες και τα queries

#### main.c
περιέχει την main συνάρτηση του προγράμματος η οποία διαβάζει τους πίνακες, διαβάζει τα queries και παράγει τα αποτέλεσμα των queries

#### execute_query.c (.h): 
Περιέχει την υλοποίηση της εκτέλεσης ενός query
* initialize_middleman: αρχικοποίει την ενδιάμεση δομή (middleman)
* filter_middle_bucket: φιλτράρει ένα bucket μιας λίστας της ενδιάμεσης δομής σύμφωνα με κάποιο φίλτρο
* filter_original_table: φιλτράρει ένα πίνακα σύμφωνα με κάποιο φίλτρο
* construct_relation_from_table: κατασκευάζει ένα relation με δεδομένη μια στήλη ενός πίνακα
* construct_relation_from_middleman: κατασκευάζει ένα relation παίρνοντας από τον αρχικό πίνακα μόνο όσα rowIds είναι μέσα στην ενδιάμεση δομή
* update_middle_bucket: μετά το join πηγαίνουμε στην ενδιάμεση δομή και για κάθε rowId του αποτελέσματος βρίσκουμε που αντιστοιχεί στον αρχικό πίνακα
* self_join_table: Self Join ενός πίνακα με βάση δύο στήλες
* self_join_middle_bucket: Indirect Self Join ενός bucket μιας λίστας του ενδιάμεσου πίνακα
* original_self_join_middle_bucket: Direct Self Join ενός bucket μιας λίστας του ενδιάμεσου πίνακα
* execute_query: εκτέλεση query

#### query.c (.h):
* create_query: δημιουργία πίνακα histogram
* delete_query: μετατροπή του histogram σε psum
* parse_predicate: τοποθέτηση των εγγραφών από τον αρχικό πίνακα στον ταξινομημένο με βάση το psum
* remove_extra_chars: υλοποίηση της αναδρομικής radix για την ταξινόμηση ενός πίνακα
* analyze_query
* print_table_column
* print_predicate_type
* print_predicate_filter_type
* print_predicate_join
* print_predicate_filter
print_predicate
print_query
print_query_like_an_str
compare_predicates
validate_query

#### radix_sort.c (.h):
* create_histogram: δημιουργία πίνακα histogram
* transform_to_psum: μετατροπή του histogram σε psum
* copy_relation_with_psum: τοποθέτηση των εγγραφών από τον αρχικό πίνακα στον ταξινομημένο με βάση το psum
* radix_sort_recursive/radix_sort: υλοποίηση της αναδρομικής radix για την ταξινόμηση ενός πίνακα

#### relation.c (.h):
* read_from_file: διαβάζει έναν πίνακα από αρχείο
* delete_table: απελευθέρωση μνήμης του πίνακα
* create_relation_from_table: δημιουργεί τον πίνακα {rowid, key}
* relation_from_file: διάβασμα των δεδομένων από ένα αρχείο για τη δημιουργία ενός relation
* relation_to_file: αποθήκευση των δεδομένων ενός relation σε ένα αρχείο
* print_relation: εκτύπωση όλων των tuples ενός relation
* print_tuples: εκτύπωση του πίνακα {rowid, key}

#### middle_list.c (.h):
Βοηθητική δομή στην οποία αποθηκεύονται τα αποτελέσματα της σύζευξης:
* create_result_list: αρχικοποίηση λίστας
* delete_result_list: απελευθέρωση μνήμης λίστας
* append_to_list: προσθήκη αποτελέσματος σύζευξης στη λίστα. Η λίστα γνωρίζει αν ένας κάδος έχει γεμίσει και δημιουργεί νέο αυτόματα
* print_result_list: εκτύπωση αποτελεσμάτων
* is_result_list_empty: επιστρέφει αν η λίστα είναι άδεια
* result_list_get_number_of_buckets: επιστρέφει τον αριθμό των κάδων
* result_list_get_number_of_records: επιστρέφει τον συνολικό αριθμό των εγγραφών σύζευξης

#### sort_merge_join.c (.h):
* final_join: διασχίζει παράλληλα δύο relations και κάνει τη σύζευξη

#### queue.c (.h):  
* create_queue: δημιουργεί ουρά
* is_empty: ελέγχει αν μια ουρά είναι κενή
* push: βάζει ένα στοιχείο μέσα στην ουρά
* pop: βγάζει ένα στοιχείο από την ουρά

#### table.c (.h): 
* read_tables: διαβάζει filenames πινάκων από το stdin και τα επιστρέφει ως λίστα
* table_from_file: επιστρέφει έναν πίνακα από ένα αρχείο
* read_ from_file
* delete_table/delete_table_contents/delete_table_index: απελευθέρωση μνήμης
* insert_tables_from_list: διαβάζει μια λίστα με ονόματα αρχείων πινάκων και επιστρέφει τους πίνακες
* get_table: επιστρέφει έναν πίνακα με βάση το id του
* insert_tables

#### Makefile:  
Makefile για την μεταγλώττιση των προγραμμάτων


    
### 2) Διαφορές με την εκφώνηση και κάποιες σχεδιαστικές επιλογές
Η υλοποίηση μας διαφέρει σε σχέση με την εκφώνηση σε τρία σημεία:
* Τα tuple χρησιμοποιούν uint64_t αντί int64_t.
* Η λίστα είναι μία απλά συνδεδεμένη λίστα με κεφαλή και δείκτη στον τελευταίο κάδο (ουρά). Κάθε κάδος της λίστας εκτός από τον buffer με μέγεθος 1MΒ και τον δείκτη στο επόμενο κάδο έχει και έναν δείκτη στην επόμενη κενή θέση του buffer ώστε σε συνδυασμό με τον δείκτη της ουράς η εισαγωγή να γίνεται σε Ο(1).


### 3) Μεταγλώττιση και χρήση των προγραμμάτων
Έχουμε θεωρήσει πως τα προγράμματα θα εκτελεστούν σύμφωνα με τις οδηγίες αυτού εδώ του εγγράφου
και για αυτό δεν έχουμε καλύψει κάθε ενδεχόμενο λάθους χρήσης, καθώς αυτό δεν ήταν το ζητούμενο της εργασίας.\
Για την μεταγλώττιση στα μηχανήματα των Linux κατεβάζετε τα αρχεία από το git και χρησιμοποιείτε τις εντολές:\
    1) Για το join: ```make ή make all```\
    2) Για τα tests: ```make tests```\
    3) Για διαγραφή των εκτελέσιμων και των αντικειμενικών (.o) αρχείων: ```make clean```
    
#### Εκτέλεση:
Μετά την μεταγλώττιση η εντολή για την εκκίνηση της εργασίας είναι: 
```
 ./join (-f)  path_to_relation_input1   path_to_relation_input2  
```
 
 - - - -
 
 ### Tests
Χρησιμοποιούμε το CUnit framework. Τα tests εκτελέστηκαν στους προσωπικούς μας υπολογιστές κατόπιν αδυναμίας εκτέλεσης τους στα μηχανήματα του Τμήματος. Για κάθε .c του πηγαίου κώδικα υπάρχει ενά αρχείο testing που ελέγχει όλες τις συναρτήσεις που περιλαμβάνονται στο πρώτο. Όπως αναφέρθηκε τα αρχεία testing μεταγλωττίζονται με το υπάρχον Makefile.

#### Εκτέλεση Tests:
Μετά την μεταγλώττιση η εντολή για την εκκίνηση των Tests είναι: 
```
 ./quicksort_test
 ./radix_sort_test
 ./relation_test
 ./result_list_test
 ./sort_merge_join_test
```
