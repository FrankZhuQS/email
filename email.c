// email v1.0 written by ChenglongGeng
#include "postgres.h"

#include "fmgr.h"
#include "libpq/pqformat.h"		/* needed for send/recv functions */
#include "access/hash.h"

#include <regex.h>
#include <string.h>
#include <ctype.h>


#define MAX_EMAIL_ADDR_LENGTH 257
#define MAX_LOCAL_LENGTH 128
#define MAX_DOMAIN_LENGTH 128

PG_MODULE_MAGIC;

/* structure of Email*/
typedef struct Email {
	char local[MAX_DOMAIN_LENGTH + 1];
	char domain[MAX_LOCAL_LENGTH + 1];
} Email; 

/* test if the input email address is valid according to RFC822 standard */
static int valid_email_addr(char *email_addr) {
	regmatch_t pmatch[1];
	const size_t nmatch = 1;
	regex_t reg;
	int MATCH;
	const char *pattern = "^[a-z][a-z0-9]*(-[a-z0-9]+)*(\\.[a-z][a-z0-9]*(-[a-z0-9]+)*)*@[a-z][a-z0-9]*(-[a-z0-9]+)*(\\.[a-z][a-z0-9]*(-[a-z0-9]+)*)+$";
	regcomp(&reg, pattern, REG_EXTENDED | REG_ICASE);
	MATCH = regexec(&reg, email_addr, nmatch, pmatch, 0);
	regfree(&reg);
	return MATCH;
}

/* transform every letter in email address into lower case */
static void canonical_email_addr(char *email_addr) {
	int i = 0;
	while (email_addr[i] != '\0' && i < MAX_EMAIL_ADDR_LENGTH) {
		email_addr[i] = tolower(email_addr[i]);
		i++;
	}
}

/* compare two email address according to lexical order */
static int inter_compare(Email *a, Email *b) {
	int cmp = strcmp(a -> domain, b -> domain);
	if (cmp != 0)
		return cmp;
	else
		return strcmp(a -> local, b -> local);
}
/* declare functions */
Datum		email_in(PG_FUNCTION_ARGS);
Datum		email_out(PG_FUNCTION_ARGS);
Datum		email_lt(PG_FUNCTION_ARGS);
Datum		email_le(PG_FUNCTION_ARGS);
Datum		email_eq(PG_FUNCTION_ARGS);
Datum		email_ne(PG_FUNCTION_ARGS);
Datum		email_ge(PG_FUNCTION_ARGS);
Datum		email_gt(PG_FUNCTION_ARGS);
Datum		email_sd(PG_FUNCTION_ARGS);
Datum		email_nd(PG_FUNCTION_ARGS);
Datum		email_cmp(PG_FUNCTION_ARGS);
Datum		email_hash(PG_FUNCTION_ARGS);


/* input/output functions */
PG_FUNCTION_INFO_V1(email_in);

Datum
email_in(PG_FUNCTION_ARGS) {
	char *email_addr = PG_GETARG_CSTRING(0);
	Email *result;
	if (strlen(email_addr) > MAX_EMAIL_ADDR_LENGTH) {
		/* over max length */
		ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid length for email (> 257): \"%s\"", email_addr)));
	} else if (valid_email_addr(email_addr) == REG_NOMATCH) {
		/* format is not up tp RFC822 standard */
		ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid input format for email: \"%s\"", email_addr)));
	} else {
		char *local = strtok(email_addr, "@");
		char *domain = strtok(NULL, "@");
		if (strlen(local) > MAX_LOCAL_LENGTH || strlen(local) > MAX_DOMAIN_LENGTH) {
			/* over max length */
			ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("invalid length for domain or local (> 128): \"%s\"", email_addr)));
		} else {
			canonical_email_addr(local);
			canonical_email_addr(domain);
			result = (Email *)palloc(sizeof(Email));
			strcpy(result -> local, local);
			strcpy(result -> domain, domain);
			PG_FREE_IF_COPY(email_addr, 0);
			PG_RETURN_POINTER(result);
		}
	}
}

PG_FUNCTION_INFO_V1(email_out);

Datum
email_out(PG_FUNCTION_ARGS) {
	Email *email = (Email *) PG_GETARG_POINTER(0);
	char *result; 
	result = psprintf("%s@%s", email -> local, email -> domain);
	PG_FREE_IF_COPY(email, 0);
	PG_RETURN_CSTRING(result);
}

PG_FUNCTION_INFO_V1(email_eq);

/* operator class for defining B-tree index */
Datum
email_eq(PG_FUNCTION_ARGS) {
	Email *a = (Email *) PG_GETARG_POINTER(0);
	Email *b = (Email *) PG_GETARG_POINTER(1);
	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_BOOL(inter_compare(a, b) == 0);
}

PG_FUNCTION_INFO_V1(email_ne);

Datum		
email_ne(PG_FUNCTION_ARGS) {
	Email *a = (Email *) PG_GETARG_POINTER(0);
	Email *b = (Email *) PG_GETARG_POINTER(1);
	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_BOOL(inter_compare(a, b) != 0);
}

PG_FUNCTION_INFO_V1(email_lt);

Datum
email_lt(PG_FUNCTION_ARGS) {
	Email *a = (Email *) PG_GETARG_POINTER(0);
	Email *b = (Email *) PG_GETARG_POINTER(1);
	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_BOOL(inter_compare(a, b) < 0);
}

PG_FUNCTION_INFO_V1(email_le);

Datum
email_le(PG_FUNCTION_ARGS) {
	Email *a = (Email *) PG_GETARG_POINTER(0);
	Email *b = (Email *) PG_GETARG_POINTER(1);
	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_BOOL(inter_compare(a, b) <= 0);
}

PG_FUNCTION_INFO_V1(email_gt);

Datum
email_gt(PG_FUNCTION_ARGS) {
	Email *a = (Email *) PG_GETARG_POINTER(0);
	Email *b = (Email *) PG_GETARG_POINTER(1);
	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_BOOL(inter_compare(a, b) > 0);
}

PG_FUNCTION_INFO_V1(email_ge);

Datum
email_ge(PG_FUNCTION_ARGS) {
	Email *a = (Email *) PG_GETARG_POINTER(0);
	Email *b = (Email *) PG_GETARG_POINTER(1);
	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_BOOL(inter_compare(a, b) >= 0);
}

/* same domain parts*/
PG_FUNCTION_INFO_V1(email_sd); 

Datum
email_sd(PG_FUNCTION_ARGS) {
	Email *a = (Email *) PG_GETARG_POINTER(0);
	Email *b = (Email *) PG_GETARG_POINTER(1);
	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_BOOL(strcmp(a -> domain, b -> domain) == 0);
}

/* different domain parts*/
PG_FUNCTION_INFO_V1(email_nd); 

Datum
email_nd(PG_FUNCTION_ARGS) {
	Email *a = (Email *) PG_GETARG_POINTER(0);
	Email *b = (Email *) PG_GETARG_POINTER(1);
	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_BOOL(strcmp(a -> domain, b -> domain) != 0);
}

PG_FUNCTION_INFO_V1(email_cmp);

Datum
email_cmp(PG_FUNCTION_ARGS) {
	Email *a = (Email *) PG_GETARG_POINTER(0);
	Email *b = (Email *) PG_GETARG_POINTER(1);
	PG_FREE_IF_COPY(a, 0);
	PG_FREE_IF_COPY(b, 1);
	PG_RETURN_INT32(inter_compare(a, b));
}

/* hash function for define hash based index*/
PG_FUNCTION_INFO_V1(email_hash);

Datum
email_hash(PG_FUNCTION_ARGS) {
	Datum result;
	Email *email = (Email *) PG_GETARG_POINTER(0);
	char *str = (char *) palloc (sizeof(char) * MAX_EMAIL_ADDR_LENGTH);
	strcpy(str, email -> local);
	strcat(str, "@");
	strcat(str, email -> domain);
	result = hash_any((unsigned char*) str, strlen(str));
	pfree(str);
	PG_FREE_IF_COPY(email, 0);
	PG_RETURN_DATUM(result);
}
