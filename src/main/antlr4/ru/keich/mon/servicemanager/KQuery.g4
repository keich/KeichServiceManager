grammar KQuery;

parse
 : expr EOF
 ;
 
 
expr 
 : STRING '=' expr_val   # ExprEqual
 | STRING '!=' expr_val  # ExprNotEqual
 | STRING '<' expr_val   # ExprLessThan
 | STRING '>' expr_val   # ExprGreaterThan
 | STRING '>=' expr_val  # ExprGreaterEqual
 | STRING '=*' expr_val  # ExprContain
 | STRING '!*' expr_val  # ExprNotContain
 | STRING '!+' expr_val  # ExprNotInclude
 | '(' expr ')'          # ExprParentheses
 | expr 'and' expr       # ExprAND
 | expr 'or' expr        # ExprOR
 ;

expr_val
 : STRING
 | num
 ;
 
STRING
 : '"' (ESC | SAFECODEPOINT)* '"'
 ;

WHITE_SPACE: [ \t\r\n]+ -> skip;

num
 : DECIMAL_LITERAL
 ;

DECIMAL_LITERAL : DEC_DIGIT+;

fragment DEC_DIGIT   : [0-9];

fragment ESC
 : '\\' (["])
 ;

fragment SAFECODEPOINT
 : ~ ["\\\u0000-\u001F]
 ;
   
