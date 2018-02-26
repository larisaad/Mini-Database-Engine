Danaila Larisa Andreea

TEMA 2 APD


Descriere solutie seriala:

Implementare bd: un hashmap<nume tabela, tabela>.
Implementare tabela: un hashmap<nume coloana, coloana>.
Implementare coloana: un arraylist<obiect>.

Functii tabela:
Implementare insert: 
	- pentru fiecare coloana din tablea, adaug un nou element la acel arraylist de
	obiecte al coloanei.

Implementare select: 
	- un obiect de tipul MyCondition va avea ca atribute: coloana pe care se verifica
	conditia, comparatorul si valoarea comparata, si are metode ce verifica daca un obiect
	face match pe obiectul MyCondition.
	- contine un switch pentru operatie in care se apeleaza metoda operatiei respective
	avand ca argumente coloana si conditia actuala.
	- metoda unei operatii parcurge toate elementele coloanei primite ca parametru si le
	ia in calcul la rezultat, doar pe cele care respecta conditia primita ca parametru.
	- se adauga rezultatul fiecarei operatii la arraylistul rezultatului final.

Implementare update:
	- pentru fiecare coloana, se verifica fiecare element si se updateaza doar cele care
	fac match cu conditia de tipul MyCondition.


Descriere solutie paralela:

Implementare scriitori-cititori:

Am folosit o implementare a problemei scriitori-cititori ce nu permite starvation.
Am folosit ReentrantReadWriteLock pentru a controla mai multi citiori (select) si mai multi
scriitori (insert, update) care fac operatii pe aceeasi tabela.

Folosesc ReentrantReadWriteLock in loc de ReadWriteLock pentru a evita deadlockurile care apar
in cazul cititorilor multipli.

Scalabilitate:

Am paralelizat updateul folosind ExecutorService.
Imi creez intotdeauna 20 de taskuri care isi impart intre ele actualizarea tabelei.
La fel paralelizez si selectul, folosesc taskuri pentru a imparti operatia de selectie a tabelei.


Se observa cum updateul scaleaza. 
Scalabilitatea la select nu se vedea la fiecare rulare. De asemenea, de cand am adaugat 
ExecutorService se intampla ca, la unele rulari, selectul, in cazul unui anumit nr. de threaduri,
sa dureze mai mult ca varianta seriala a acestuia si ca testele de consistency select/insert sa nu treaca pe local, dar treceau pe fep.

Voi uploada varianta cu selectul neparalizat, pentru a evita problemele de mai sus, care scaleaza.

Am testat pe coada ibm-nehalem.q.

Select/Insert Consistency PASS
Select/Update Consistency PASS
Transactions Consistency PASS
There are now 1 Threads
[[532894452]]
Insert time 67964
Update time 531
Select time 3415
There are now 2 Threads
[[532894452]]
Insert time 50142
Update time 198
Select time 2410
There are now 4 Threads
[[532894452]]
Insert time 39781
Update time 167
Select time 2078






