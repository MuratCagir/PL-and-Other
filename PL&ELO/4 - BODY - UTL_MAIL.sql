CREATE OR REPLACE PACKAGE BODY SYS.UTL_MAIL IS
   
     TYPE SMTP_SERVERS_T IS TABLE OF VARCHAR2(512);
     TYPE SMTP_PORTS_T   IS TABLE OF PLS_INTEGER;
   
     LONG_HEADER_FIELD     CONSTANT INTEGER := 65;
     SUBJECT_PIECE_LENGTH  CONSTANT INTEGER := 40;
     
     BOUNDARY CONSTANT VARCHAR2(256) := '------------4D8C24=_23F7E4A13B2357B3';
   
     
     DEFAULT_PORT          CONSTANT PLS_INTEGER := 25;
   
     
     BAD_ARGUMENT  EXCEPTION;
     PRAGMA EXCEPTION_INIT(BAD_ARGUMENT, -29261);
   
     
   
     
     
     
     FUNCTION GET_MAIL_BANNER 
       RETURN VARCHAR2 IS
       MAIL_BANNER VARCHAR2(128);
     BEGIN
       SELECT 'Mailer by '||
              SUBSTR(BANNER,1,INSTR(BANNER,' ')-1)||
              ' UTL_MAIL'
       INTO   MAIL_BANNER 
       FROM   V$VERSION
       WHERE  ROWNUM<2;
   
       RETURN MAIL_BANNER;
     END;
   
     
     
     
     
     
     
     
     
     FUNCTION LOOKUP_UNQUOTED_CHAR(STR  IN VARCHAR2 CHARACTER SET ANY_CS,
                                   CHRS IN VARCHAR2) RETURN PLS_INTEGER AS
       C            VARCHAR2(5) CHARACTER SET STR%CHARSET;
       I            PLS_INTEGER;
       LEN          PLS_INTEGER;
       INSIDE_QUOTE BOOLEAN;
     BEGIN
        INSIDE_QUOTE := FALSE;
        I := 1;
        LEN := LENGTH(STR);
        WHILE (I <= LEN) LOOP
   
          C := SUBSTR(STR, I, 1);
   
          IF (INSIDE_QUOTE) THEN
            IF (C = '"') THEN
              INSIDE_QUOTE := FALSE;
            ELSIF (C = '\') THEN
              I := I + 1; 
            END IF;
            GOTO NEXT_CHAR;
          END IF;
          
          IF (C = '"') THEN
            INSIDE_QUOTE := TRUE;
            GOTO NEXT_CHAR;
          END IF;
         
          IF (INSTR(CHRS, C) >= 1) THEN
             RETURN I;
          END IF;
       
          <<NEXT_CHAR>>
          I := I + 1;
   
        END LOOP;
     
        RETURN 0;
       
     END;
   
     
     
      
     
     
     
     
     
     FUNCTION GET_ADDRESS(ADDR_LIST IN OUT VARCHAR2) RETURN VARCHAR2 IS
   
       ADDR VARCHAR2(256);
       I    PLS_INTEGER;
   
   
     BEGIN
   
       ADDR_LIST := LTRIM(ADDR_LIST);
       I := LOOKUP_UNQUOTED_CHAR(ADDR_LIST, ',;');
       IF (I >= 1) THEN
         ADDR      := SUBSTR(ADDR_LIST, 1, I - 1);
         ADDR_LIST := SUBSTR(ADDR_LIST, I + 1);
       ELSE
         ADDR := ADDR_LIST;
         ADDR_LIST := '';
       END IF;
      
       I := LOOKUP_UNQUOTED_CHAR(ADDR, '<');
       IF (I >= 1) THEN
         ADDR := SUBSTR(ADDR, I + 1);
         I := INSTR(ADDR, '>');
         IF (I >= 1) THEN
           ADDR := SUBSTR(ADDR, 1, I - 1);
         END IF;
       END IF;
       RETURN ADDR;
     END;
   
     
     
     
     FUNCTION ENCODE_VARCHAR2(DATA IN VARCHAR2 CHARACTER SET ANY_CS) 
       RETURN VARCHAR2 IS
     BEGIN
         RETURN UTL_RAW.CAST_TO_VARCHAR2(
                  UTL_ENCODE.QUOTED_PRINTABLE_ENCODE(
                    UTL_RAW.CAST_TO_RAW(DATA)));
     END;
   
     
     
     
     FUNCTION ENCODE_RAW(DATA IN RAW) RETURN RAW IS
     BEGIN
       RETURN UTL_ENCODE.BASE64_ENCODE(DATA);
     END;
   
     
     
     
     FUNCTION ENCODE_HEADER(DATA IN VARCHAR2 CHARACTER SET ANY_CS)
       RETURN VARCHAR2 CHARACTER SET DATA%CHARSET IS
     BEGIN
         RETURN (UTL_ENCODE.MIMEHEADER_ENCODE(DATA));
     END;
   
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
   
     FUNCTION ENCODE_RECIPIENTS(RCPTS IN VARCHAR2 CHARACTER SET ANY_CS) 
       RETURN VARCHAR2 CHARACTER SET RCPTS%CHARSET IS
       START_LOC            PLS_INTEGER := 1;
       OCCUR_LOC            PLS_INTEGER := 1;
       SINGLE_RCPT          VARCHAR2(32767) CHARACTER SET RCPTS%CHARSET;
       ENCODED_RCPTS        VARCHAR2(32767) CHARACTER SET RCPTS%CHARSET;
       REMAINING_RCPTS      VARCHAR2(32767) CHARACTER SET RCPTS%CHARSET;
       ADDRESS_SEP          VARCHAR2(1)     CHARACTER SET RCPTS%CHARSET;
       ENCODED_SINGLE_RCPT  VARCHAR2(32767) CHARACTER SET RCPTS%CHARSET;
   
       FUNCTION ENCODE_SINGLE_RCPT(SINGLE_RCPT IN  VARCHAR2 CHARACTER SET ANY_CS)
         RETURN VARCHAR2 CHARACTER SET SINGLE_RCPT%CHARSET IS
   
         SEPARATOR_LOC        PLS_INTEGER := 0;
         START_LOC            PLS_INTEGER := 0;
         SINGLE_RCPT_PIECE1   VARCHAR2(32767) CHARACTER SET SINGLE_RCPT%CHARSET;
         SINGLE_RCPT_PIECE2   VARCHAR2(32767) CHARACTER SET SINGLE_RCPT%CHARSET;
         ENCODED_SINGLE_RCPT  VARCHAR2(32767) CHARACTER SET SINGLE_RCPT%CHARSET;
         PIECE1_SPLIT         VARCHAR2(32767) CHARACTER SET SINGLE_RCPT%CHARSET;
         ENCODED_PIECE        VARCHAR2(32767) CHARACTER SET SINGLE_RCPT%CHARSET;
   
       BEGIN
         
         
         
         
         
         
         
         
         ENCODED_SINGLE_RCPT := SINGLE_RCPT;
         SEPARATOR_LOC := LOOKUP_UNQUOTED_CHAR(SINGLE_RCPT, '<');
   
         IF (SEPARATOR_LOC >= 1) THEN
           ENCODED_SINGLE_RCPT := NULL;
           SINGLE_RCPT_PIECE1 := SUBSTR(SINGLE_RCPT, 1, SEPARATOR_LOC -1);
           SINGLE_RCPT_PIECE2 := SUBSTR(SINGLE_RCPT, SEPARATOR_LOC);
   
           IF (SINGLE_RCPT_PIECE1 IS NOT NULL) THEN
             ENCODED_SINGLE_RCPT := ENCODE_HEADER(SINGLE_RCPT_PIECE1);
   
             IF (LENGTH(ENCODED_SINGLE_RCPT) <= LONG_HEADER_FIELD) THEN 
               ENCODED_SINGLE_RCPT := ENCODED_SINGLE_RCPT || UTL_TCP.CRLF || ' ';
             ELSE
               
               SEPARATOR_LOC := 0;
               START_LOC := 1;
               ENCODED_SINGLE_RCPT := NULL;
               LOOP
                 SEPARATOR_LOC := INSTR(SINGLE_RCPT_PIECE1, ' ', START_LOC);
                 EXIT WHEN (SEPARATOR_LOC = 0);
                 PIECE1_SPLIT := SUBSTR(SINGLE_RCPT_PIECE1, START_LOC, 
                                        SEPARATOR_LOC-START_LOC + 1 );
                 START_LOC := SEPARATOR_LOC + 1;
                 IF (PIECE1_SPLIT IS NOT NULL) THEN
                   ENCODED_PIECE := ENCODE_HEADER(PIECE1_SPLIT);
                   IF (LENGTH(ENCODED_PIECE) > LONG_HEADER_FIELD) THEN
                     
                     GOTO REPORT_ERROR;
                   END IF;
                   ENCODED_SINGLE_RCPT :=  ENCODED_SINGLE_RCPT ||
                                           ENCODED_PIECE ||
                                           UTL_TCP.CRLF || ' ';
                 END IF;
               END LOOP;
               
               PIECE1_SPLIT := SUBSTR(SINGLE_RCPT_PIECE1, START_LOC, 
                                      LENGTH(SINGLE_RCPT_PIECE1) -
                                        START_LOC + 1 );
               IF (PIECE1_SPLIT IS NOT NULL) THEN
                 ENCODED_PIECE := ENCODE_HEADER(PIECE1_SPLIT);
   
                 IF (LENGTH(ENCODED_PIECE) > LONG_HEADER_FIELD) THEN
                   
                   GOTO REPORT_ERROR;
                 END IF;
                 ENCODED_SINGLE_RCPT :=  ENCODED_SINGLE_RCPT ||
                                         ENCODED_PIECE ||
                                         UTL_TCP.CRLF || ' ';
               END IF;
             END IF;
           END IF;  
           
           
           IF (LENGTH(SINGLE_RCPT_PIECE2) > LONG_HEADER_FIELD) THEN
               GOTO REPORT_ERROR;
           END IF;
           ENCODED_SINGLE_RCPT := ENCODED_SINGLE_RCPT ||
                                  SINGLE_RCPT_PIECE2;
         END IF;  
   
         RETURN ENCODED_SINGLE_RCPT;
   
       <<REPORT_ERROR>>
       RAISE BAD_ARGUMENT;
       END;
   
     BEGIN
       ENCODED_RCPTS :=  NULL;
       REMAINING_RCPTS := RCPTS;
       LOOP
         
         OCCUR_LOC := LOOKUP_UNQUOTED_CHAR(REMAINING_RCPTS, ';,');
         EXIT WHEN (OCCUR_LOC = 0);
         SINGLE_RCPT := SUBSTR(REMAINING_RCPTS, 1, OCCUR_LOC - 1);
         ADDRESS_SEP := SUBSTR(REMAINING_RCPTS, OCCUR_LOC, 1);
         
         REMAINING_RCPTS := SUBSTR(REMAINING_RCPTS, OCCUR_LOC+1);
   
         IF (SINGLE_RCPT IS NOT NULL) THEN
           
           ENCODED_SINGLE_RCPT := ENCODE_SINGLE_RCPT(SINGLE_RCPT);
           ENCODED_RCPTS := ENCODED_RCPTS || 
                            ENCODED_SINGLE_RCPT || 
                            ADDRESS_SEP || UTL_TCP.CRLF || ' ';
         END IF;
       END LOOP;
       
       IF (REMAINING_RCPTS IS NOT NULL) THEN
         ENCODED_SINGLE_RCPT := ENCODE_SINGLE_RCPT(REMAINING_RCPTS);
         ENCODED_RCPTS := ENCODED_RCPTS || ENCODED_SINGLE_RCPT;
       END IF;
       RETURN ENCODED_RCPTS;
     END;
   
     
     
     
     PROCEDURE GET_SMTP_CONFIG(SMTP_SERVERS  OUT SMTP_SERVERS_T,
                               SMTP_PORTS    OUT SMTP_PORTS_T) IS
       SMTP_SERVER_LIST VARCHAR2(32767);
       SERVER_CLAUSE    VARCHAR2(512);
       I                PLS_INTEGER := 1;
       COMMA            PLS_INTEGER;
       COLON            PLS_INTEGER;
       DEBUG            CONSTANT BOOLEAN := FALSE;
     BEGIN
       SMTP_SERVERS  := SMTP_SERVERS_T();
       SMTP_PORTS    := SMTP_PORTS_T();
   
       SYS.UTL_MAIL_INTERNAL.GET_SMTP_SERVER(SMTP_SERVER_LIST);
       
       
       WHILE SMTP_SERVER_LIST IS NOT NULL LOOP
   
         SMTP_SERVERS.EXTEND;
         SMTP_PORTS.EXTEND;
   
         
         COMMA := INSTR(SMTP_SERVER_LIST,',');
         IF (COMMA > 0) THEN
           SERVER_CLAUSE    := TRIM(SUBSTR(SMTP_SERVER_LIST,1,COMMA-1));
           SMTP_SERVER_LIST := TRIM(SUBSTR(SMTP_SERVER_LIST,COMMA+1,
                                           LENGTH(SMTP_SERVER_LIST)));
         ELSE 
           SERVER_CLAUSE    := TRIM(SMTP_SERVER_LIST);
           SMTP_SERVER_LIST := NULL;
         END IF;
   
         
         COLON := INSTR(SERVER_CLAUSE,':');
         IF (COLON != 0) THEN
           SMTP_SERVERS(I) := TRIM(SUBSTR(SERVER_CLAUSE,1,COLON-1));
           SMTP_PORTS(I)   := TO_NUMBER(SUBSTR(SERVER_CLAUSE,COLON+1,
                                               LENGTH(SERVER_CLAUSE)));
         ELSE
           SMTP_SERVERS(I) := TRIM(SERVER_CLAUSE);
           SMTP_PORTS(I)   := DEFAULT_PORT;
         END IF;
   
         I := I + 1;
       END LOOP;
   
       IF (DEBUG) THEN
         IF SMTP_SERVERS IS NULL THEN
           DBMS_OUTPUT.PUT_LINE('smtp_servers is atomically NULL!?!');
         ELSIF SMTP_SERVERS.COUNT = 0 THEN
           DBMS_OUTPUT.PUT_LINE('smtp_servers is empty!?!');
         ELSE
           FOR I IN SMTP_SERVERS.FIRST .. SMTP_SERVERS.LAST LOOP
             DBMS_OUTPUT.PUT_LINE('Clause #' || I || ': '              ||
                                  'server=' || SMTP_SERVERS(I) || ', ' ||
                                  'port=' || SMTP_PORTS(I) || '.');
           END LOOP;
         END IF;
       END IF;
   
     END;
   
     
     
     
     PROCEDURE SEND_I(SENDER         IN VARCHAR2 CHARACTER SET ANY_CS,
                      RECIPIENTS     IN VARCHAR2 CHARACTER SET ANY_CS,
                      CC             IN VARCHAR2 CHARACTER SET ANY_CS,
                      BCC            IN VARCHAR2 CHARACTER SET ANY_CS,
                      SUBJECT        IN VARCHAR2 CHARACTER SET ANY_CS,
                      MESSAGE        IN VARCHAR2 CHARACTER SET ANY_CS,
                      MIME_TYPE      IN VARCHAR2 CHARACTER SET ANY_CS, 
                      PRIORITY       IN PLS_INTEGER,
                      TXT_ATTACHMENT IN VARCHAR2 CHARACTER SET ANY_CS,
                      RAW_ATTACHMENT IN RAW,
                      ATT_MIME_TYPE  IN VARCHAR2 CHARACTER SET ANY_CS,
                      ATT_INLINE     IN BOOLEAN,
                      ATT_FILENAME   IN VARCHAR2 CHARACTER SET ANY_CS,
                      REPLYTO        IN VARCHAR2 CHARACTER SET ANY_CS) IS 
   
       SMTP_SERVERS      SMTP_SERVERS_T;
       SMTP_PORTS        SMTP_PORTS_T;
       SMTP_SERVER       VARCHAR2(512);
       SMTP_PORT         PLS_INTEGER;
       I                 PLS_INTEGER;
       MAIL_CONN         UTL_SMTP.CONNECTION;
       CRLF              VARCHAR2(10) := UTL_TCP.CRLF;
       MSG               VARCHAR2(32767);
       ATTACH_FLAG       PLS_INTEGER := 0;  
       MAIL_BANNER       VARCHAR2(128);
       TEXT_TYPE         NUMBER := 1;
       RAW_TYPE          NUMBER := 2;
       SUBJECT_TEMP      VARCHAR2(32767);
       NONE_TYPE         NUMBER := 0;
       SENDER_COPY       VARCHAR2(32767) := SENDER;
       PRIORITY_COPY     PLS_INTEGER := PRIORITY;
       ALL_RCPTS         VARCHAR2(32767) := RECIPIENTS; 
                                                        
       CONNECTION_OPENED BOOLEAN := FALSE;
     BEGIN
       
       IF (TXT_ATTACHMENT IS NOT NULL) THEN
         ATTACH_FLAG := TEXT_TYPE;
       ELSE IF (RAW_ATTACHMENT IS NOT NULL) THEN
              ATTACH_FLAG := RAW_TYPE;
            END IF;
       END IF;
   
       
       
       
       
       
       
       GET_SMTP_CONFIG(SMTP_SERVERS, SMTP_PORTS);
   
       
       
       
       FOR I IN SMTP_SERVERS.FIRST .. SMTP_SERVERS.LAST LOOP
         BEGIN
           SMTP_SERVER := SMTP_SERVERS(I);
           SMTP_PORT   := SMTP_PORTS(I);
           MAIL_CONN := UTL_SMTP.OPEN_CONNECTION(SMTP_SERVER,SMTP_PORT);
         EXCEPTION
           WHEN OTHERS THEN
             
             IF I = SMTP_SERVERS.LAST THEN
               RAISE;
             ELSE
               CONTINUE;
             END IF;
         END;
   
         
         CONNECTION_OPENED := TRUE;
         EXIT;
       END LOOP;
   
       UTL_SMTP.HELO(MAIL_CONN, SMTP_SERVER);
       UTL_SMTP.MAIL(MAIL_CONN, GET_ADDRESS(SENDER_COPY));
   
       
       MAIL_BANNER := GET_MAIL_BANNER;
   
       
       
       
       
       
       
       IF (CC IS NOT NULL) THEN
         ALL_RCPTS := ALL_RCPTS||', '||CC;
       END IF;
       IF (BCC IS NOT NULL) THEN
         ALL_RCPTS := ALL_RCPTS||', '||BCC;
       END IF;
   
       
       WHILE (ALL_RCPTS IS NOT NULL) LOOP
         UTL_SMTP.RCPT(MAIL_CONN,'<' || GET_ADDRESS(ALL_RCPTS) || '>');
       END LOOP;
   
       UTL_SMTP.OPEN_DATA(MAIL_CONN);
   
       
       IF (SENDER IS NOT NULL) THEN
         UTL_SMTP.WRITE_DATA(MAIL_CONN, 'From: '||ENCODE_RECIPIENTS(SENDER)||CRLF);
       ELSE
         RAISE BAD_ARGUMENT;
       END IF;
   
       IF (RECIPIENTS IS NOT NULL) THEN
         UTL_SMTP.WRITE_DATA(MAIL_CONN, 'To: '||ENCODE_RECIPIENTS(RECIPIENTS)||CRLF);
       ELSE
         RAISE BAD_ARGUMENT;
       END IF;
   
       IF (CC IS NOT NULL) THEN
         UTL_SMTP.WRITE_DATA(MAIL_CONN, 
                            'CC: '||ENCODE_RECIPIENTS(CC)||CRLF);
       END IF;
       
       IF (REPLYTO IS NOT NULL) THEN
         UTL_SMTP.WRITE_DATA(MAIL_CONN, 'Reply-To: ' 
                                         ||ENCODE_RECIPIENTS(REPLYTO)||CRLF);
       END IF;
   
       
       
       
       UTL_SMTP.WRITE_DATA(MAIL_CONN, 'Orig-Date: ' || 
                           TO_CHAR(CURRENT_TIMESTAMP, 
                                   'Dy Mon YYYY HH24:MI:SS TZHTZM') || 
                           CRLF);
   
       
       
   
       
       
       
       
   
       SUBJECT_TEMP := SUBJECT;
   
       DECLARE
         CURRENTPIECE      VARCHAR2(32767) := NULL;
         ENCODEDPIECE      VARCHAR2(32767) := NULL;
         PIECELENGTH       NUMBER := SUBJECT_PIECE_LENGTH;
         REMAININGLENGTH   NUMBER := LENGTH(SUBJECT_TEMP);
         FIRSTPIECE        BOOLEAN := TRUE;
       BEGIN
         
         
   
         WHILE(REMAININGLENGTH > 0) LOOP
             
             
             CURRENTPIECE := SUBSTR(SUBJECT_TEMP, 1, PIECELENGTH);
   
             
             ENCODEDPIECE := ENCODE_HEADER(CURRENTPIECE);
   
             
             
             IF (LENGTH(ENCODEDPIECE) <= LONG_HEADER_FIELD) THEN
               IF (FIRSTPIECE = FALSE) THEN 
                 UTL_SMTP.WRITE_DATA(MAIL_CONN, ' ' || ENCODEDPIECE || CRLF);
               ELSE
                
                 UTL_SMTP.WRITE_DATA(MAIL_CONN, 
                                     'Subject: '|| ENCODEDPIECE || CRLF);
                 FIRSTPIECE := FALSE;
               END IF;
   
               
               SUBJECT_TEMP := SUBSTR(SUBJECT_TEMP, PIECELENGTH + 1,
                                      REMAININGLENGTH);
               REMAININGLENGTH := LENGTH(SUBJECT_TEMP);
               
               PIECELENGTH := SUBJECT_PIECE_LENGTH;
             ELSE
               
               
               PIECELENGTH := PIECELENGTH / 2;
               
               
               IF (PIECELENGTH = 0) THEN 
                 RAISE BAD_ARGUMENT;
               END IF;
             END IF;
         END LOOP;
       END;
   
   
   
       
       IF (PRIORITY IS NOT NULL) THEN
         IF((PRIORITY > 5) OR (PRIORITY < 1)) THEN
           RAISE INVALID_PRIORITY;
         END IF;
         UTL_SMTP.WRITE_DATA(MAIL_CONN,
                             'X-Priority: '||PRIORITY_COPY||CRLF);
       END IF;
   
       
       
       IF (ATTACH_FLAG > NONE_TYPE) THEN
         UTL_SMTP.WRITE_DATA(MAIL_CONN,
                             'Content-Type: multipart/mixed;'||CRLF);
         UTL_SMTP.WRITE_DATA(MAIL_CONN,
                             ' boundary="'||BOUNDARY||'"'||CRLF);
         UTL_SMTP.WRITE_DATA(MAIL_CONN, CRLF);
         UTL_SMTP.WRITE_DATA(MAIL_CONN,
                             'This is a multi-part message in MIME format.'||
                              CRLF);
         UTL_SMTP.WRITE_DATA(MAIL_CONN, '--'||BOUNDARY||CRLF);
       END IF;
   
       UTL_SMTP.WRITE_DATA(MAIL_CONN,
                           'Content-Type: '||MIME_TYPE||CRLF);
   
       
       
       
       IF ((MESSAGE IS NULL) OR
           (INSTR(UPPER(MIME_TYPE), 'CHARSET') = 0) OR
           (INSTR(UPPER(MIME_TYPE), 'US-ASCII') != 0)) THEN
         UTL_SMTP.WRITE_DATA(MAIL_CONN,
                            'Content-Transfer-Encoding: 7bit'||CRLF);
         UTL_SMTP.WRITE_DATA(MAIL_CONN, CRLF);
         
         UTL_SMTP.WRITE_DATA(MAIL_CONN, NVL(MESSAGE,' ')||CRLF||CRLF||CRLF);
       ELSE
         UTL_SMTP.WRITE_DATA(MAIL_CONN,
                             'Content-Transfer-Encoding: quoted-printable' ||
                              CRLF);
         UTL_SMTP.WRITE_DATA(MAIL_CONN, CRLF);
         
         UTL_SMTP.WRITE_DATA(MAIL_CONN, ENCODE_VARCHAR2(MESSAGE)||
                                        CRLF||CRLF||CRLF);
       END IF;
   
       
       IF (ATTACH_FLAG > NONE_TYPE) THEN
         UTL_SMTP.WRITE_DATA(MAIL_CONN,'--'||BOUNDARY||CRLF);
         UTL_SMTP.WRITE_DATA(MAIL_CONN,'Content-Type: '||ATT_MIME_TYPE||';'||CRLF);
         UTL_SMTP.WRITE_DATA(MAIL_CONN,' name="'||
                                        NVL(ATT_FILENAME,' ')||'"'||CRLF);
   
         
         
         
         
         
         IF (ATTACH_FLAG = TEXT_TYPE) THEN
           UTL_SMTP.WRITE_DATA(MAIL_CONN,
              'Content-Transfer-Encoding: quoted-printable'||CRLF);
         ELSE
           UTL_SMTP.WRITE_DATA(MAIL_CONN,
              'Content-Transfer-Encoding: base64'||CRLF);
         END IF;
   
         IF (ATT_INLINE) THEN
           UTL_SMTP.WRITE_DATA(MAIL_CONN, 
                               'Content-Disposition: inline;'||CRLF);
           UTL_SMTP.WRITE_DATA(MAIL_CONN, 
                               ' filename="'||NVL(ATT_FILENAME,' ')||'"'||
                               CRLF||CRLF||CRLF);
         ELSE
           UTL_SMTP.WRITE_DATA(MAIL_CONN, 
                               'Content-Disposition: attachment;'||CRLF);
           UTL_SMTP.WRITE_DATA(MAIL_CONN, 
                               ' filename="'||NVL(ATT_FILENAME,' ')||'"'||
                               CRLF||CRLF||CRLF);
         END IF;
        
         IF (ATTACH_FLAG = TEXT_TYPE) THEN
           UTL_SMTP.WRITE_DATA(MAIL_CONN, ENCODE_VARCHAR2(TXT_ATTACHMENT));
         ELSE
           UTL_SMTP.WRITE_RAW_DATA(MAIL_CONN, ENCODE_RAW(RAW_ATTACHMENT));
         END IF;
     
         UTL_SMTP.WRITE_DATA(MAIL_CONN, CRLF);
         UTL_SMTP.WRITE_DATA(MAIL_CONN, '--'||BOUNDARY||'--'||CRLF);
   
       END IF;  
   
       
       UTL_SMTP.CLOSE_DATA(MAIL_CONN);
       UTL_SMTP.QUIT(MAIL_CONN);
       
     EXCEPTION
       WHEN OTHERS THEN
         IF (CONNECTION_OPENED) THEN
           UTL_SMTP.CLOSE_CONNECTION(MAIL_CONN);
         END IF;
         RAISE;
     END;
   
   
     
     PROCEDURE SEND (SENDER     IN VARCHAR2 CHARACTER SET ANY_CS,
                     RECIPIENTS IN VARCHAR2 CHARACTER SET ANY_CS,
                     CC         IN VARCHAR2 CHARACTER SET ANY_CS DEFAULT NULL,
                     BCC        IN VARCHAR2 CHARACTER SET ANY_CS DEFAULT NULL,
                     SUBJECT    IN VARCHAR2 CHARACTER SET ANY_CS DEFAULT NULL,
                     MESSAGE    IN VARCHAR2 CHARACTER SET ANY_CS DEFAULT NULL,
                     MIME_TYPE  IN VARCHAR2 CHARACTER SET ANY_CS 
                                   DEFAULT 'text/plain; charset=us-ascii',
                     PRIORITY   IN PLS_INTEGER DEFAULT 3,
                     REPLYTO        IN VARCHAR2 CHARACTER SET ANY_CS DEFAULT NULL) 
     IS
     BEGIN
       SEND_I(SENDER,
              RECIPIENTS,
              CC,
              BCC,
              SUBJECT,
              MESSAGE,
              MIME_TYPE,
              PRIORITY,
              NULL,
              NULL,
              NULL,
              NULL,
              NULL,
              REPLYTO);
     END;
   
     PROCEDURE SEND_ATTACH_VARCHAR2 (
                     SENDER         IN VARCHAR2 CHARACTER SET ANY_CS,
                     RECIPIENTS     IN VARCHAR2 CHARACTER SET ANY_CS,
                     CC             IN VARCHAR2 CHARACTER SET ANY_CS DEFAULT NULL,
                     BCC            IN VARCHAR2 CHARACTER SET ANY_CS DEFAULT NULL,
                     SUBJECT        IN VARCHAR2 CHARACTER SET ANY_CS DEFAULT NULL,
                     MESSAGE        IN VARCHAR2 CHARACTER SET ANY_CS DEFAULT NULL,
                     MIME_TYPE      IN VARCHAR2 CHARACTER SET ANY_CS 
                                       DEFAULT 'text/plain; charset=us-ascii',
                     PRIORITY       IN PLS_INTEGER DEFAULT 3,
                     ATTACHMENT     IN VARCHAR2 CHARACTER SET ANY_CS,
                     ATT_INLINE     IN BOOLEAN DEFAULT TRUE,
                     ATT_MIME_TYPE  IN VARCHAR2 CHARACTER SET ANY_CS 
                                       DEFAULT 'text/plain; charset=us-ascii',
                     ATT_FILENAME   IN VARCHAR2 CHARACTER SET ANY_CS DEFAULT NULL,
                     REPLYTO        IN VARCHAR2 CHARACTER SET ANY_CS DEFAULT NULL)
     IS
     BEGIN
       SEND_I(SENDER,
              RECIPIENTS,
              CC,
              BCC,
              SUBJECT,
              MESSAGE,
              MIME_TYPE,
              PRIORITY,
              ATTACHMENT,
              NULL,
              ATT_MIME_TYPE,
              ATT_INLINE,
              ATT_FILENAME,
              REPLYTO);
     END;
   
     PROCEDURE SEND_ATTACH_RAW (
                     SENDER         IN VARCHAR2 CHARACTER SET ANY_CS,
                     RECIPIENTS     IN VARCHAR2 CHARACTER SET ANY_CS,
                     CC             IN VARCHAR2 CHARACTER SET ANY_CS DEFAULT NULL,
                     BCC            IN VARCHAR2 CHARACTER SET ANY_CS DEFAULT NULL,
                     SUBJECT        IN VARCHAR2 CHARACTER SET ANY_CS DEFAULT NULL,
                     MESSAGE        IN VARCHAR2 CHARACTER SET ANY_CS DEFAULT NULL,
                     MIME_TYPE      IN VARCHAR2 CHARACTER SET ANY_CS
                                       DEFAULT 'text/plain; charset=us-ascii',
                     PRIORITY       IN PLS_INTEGER DEFAULT 3,
                     ATTACHMENT     IN RAW,
                     ATT_INLINE     IN BOOLEAN DEFAULT TRUE,
                     ATT_MIME_TYPE  IN VARCHAR2 CHARACTER SET ANY_CS 
                                       DEFAULT 'application/octet',
                     ATT_FILENAME   IN VARCHAR2 CHARACTER SET ANY_CS DEFAULT NULL,
                     REPLYTO        IN VARCHAR2 CHARACTER SET ANY_CS DEFAULT NULL)
     IS
     BEGIN
       SEND_I(SENDER,
              RECIPIENTS,
              CC,
              BCC,
              SUBJECT,
              MESSAGE,
              MIME_TYPE,
              PRIORITY,
              NULL,
              ATTACHMENT,
              ATT_MIME_TYPE,
              ATT_INLINE,
              ATT_FILENAME,
              REPLYTO);
     END;
   
   END;
/
