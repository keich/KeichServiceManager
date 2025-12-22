grammar KQuery;

parse
 : expr EOF
 ;
 
 
expr 
 : expr_props '='  expr_val        # ExprEqual
 | expr_props '!=' expr_val        # ExprNotEqual
 | expr_props '<'  expr_val        # ExprLessThan
 | expr_props '>'  expr_val        # ExprGreaterThan
 | expr_props '>=' expr_val        # ExprGreaterEqual
 | expr_props '=*' expr_val        # ExprContain
 | expr_props '!*' expr_val        # ExprNotContain
 | expr_props '!+' expr_val        # ExprNotInclude
 | expr_props IN '(' expr_str_list ')' # ExprInEqual
 | FIELDS '.' STRING '='  STRING   # ExprFieldsEqual
 | FIELDS '.' STRING '=*' STRING   # ExprFieldsContain
 | '(' expr ')'                    # ExprParentheses
 | expr AND expr                 # ExprAND
 | expr OR expr                  # ExprOR
 ;

expr_val
 : STRING
 | num
 | base_status
 ;
 
expr_str_list
 : STRING
 | STRING ',' expr_str_list
 ;
 
expr_props
 : NAME
 | VERSION
 | SOURCE
 | SOURCEKEY
 | SOURCETYPE
 | STATUS
 | FIELDS
 | FROMHISTORY
 | CREATEDON
 | UPDATEDON
 | DELETEDON
 | AGGSTATUS
 | SUMMARY
 | NODE
 ;

base_status
 : CLEAR
 | INDETERMINATE
 | INFORMATION
 | WARNING
 | MAJOR
 | CRITICAL
 ;

AND: A N D;
OR: O R;
IN: I N;

CLEAR: C L E A R;
INDETERMINATE: I N D E T E R M I N A T E;
INFORMATION: I N F O R M A T I O N;
WARNING: W A R N I N G;
MAJOR: M A J O R;
CRITICAL: C R I T I C A L;
SUMMARY: S U M M A R Y;
NODE: N O D E;

VERSION: V E R S I O N;
SOURCE: S O U R C E;
SOURCEKEY: S O U R C E K E Y;
SOURCETYPE: S O U R C E T Y P E; 
STATUS: S T A T U S;
NAME: N A M E;
FIELDS: F I E L D S;
FROMHISTORY: F R O M H I S T O R Y;
CREATEDON: C R E A T E D O N;
UPDATEDON: U P D A T E D O N;
DELETEDON: D L E T E D O N;
AGGSTATUS: A G G S T A T U S;
 
STRING
 : '"' (ESC | SAFECODEPOINT)* '"'
 ;

WHITE_SPACE: [ \t\r\n]+ -> skip;

num
 : DECIMAL_LITERAL
 ;

DECIMAL_LITERAL : DEC_DIGIT+;

fragment A: [aA];
fragment B: [bB];
fragment C: [cC];
fragment D: [dD];
fragment E: [eE];
fragment F: [fF];
fragment G: [gG];
fragment H: [hH];
fragment I: [iI];
fragment J: [jJ];
fragment K: [kK];
fragment L: [lL];
fragment M: [mM];
fragment N: [nN];
fragment O: [oO];
fragment P: [pP];
fragment Q: [qQ];
fragment R: [rR];
fragment S: [sS];
fragment T: [tT];
fragment U: [uU];
fragment V: [vV];
fragment W: [wW];
fragment X: [xX];
fragment Y: [yY];
fragment Z: [zZ];

fragment DEC_DIGIT   : [0-9];

fragment ESC
 : '\\' (["])
 ;

fragment SAFECODEPOINT
 : ~ ["\\\u0000-\u001F]
 ;
   
