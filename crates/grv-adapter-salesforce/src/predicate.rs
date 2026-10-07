//! SOQL row-predicate grammar. Field traversal and scalar function calls remain
//! available; relation queries, trailing query clauses, and bind variables do
//! not form row predicates. The generated query preserves the author's text.
//! Grammar: https://developer.salesforce.com/docs/platform/salesforce-soql-sosl/guide/sforce-api-calls-soql-select-conditionexpression.html
use crate::{
    Result,
    config::{PredicateCompiler, field_path},
    invalid,
};

#[derive(Default)]
pub struct SoqlPredicateCompiler;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Token<'a> {
    Word(&'a str),
    Literal,
    Left,
    Right,
    Comma,
    Operator,
}

fn numeric(value: &str) -> bool {
    let mut chars = value.bytes().peekable();
    if matches!(chars.peek(), Some(b'+' | b'-')) {
        chars.next();
    }
    let mut digits = 0;
    while chars.peek().is_some_and(u8::is_ascii_digit) {
        chars.next();
        digits += 1;
    }
    if digits == 0 {
        return false;
    }
    if chars.peek() == Some(&b'.') {
        chars.next();
        let mut fractional = 0;
        while chars.peek().is_some_and(u8::is_ascii_digit) {
            chars.next();
            fractional += 1;
        }
        if fractional == 0 {
            return false;
        }
    }
    if matches!(chars.peek(), Some(b'e' | b'E')) {
        chars.next();
        if matches!(chars.peek(), Some(b'+' | b'-')) {
            chars.next();
        }
        let mut exponent = 0;
        while chars.peek().is_some_and(u8::is_ascii_digit) {
            chars.next();
            exponent += 1;
        }
        if exponent == 0 {
            return false;
        }
    }
    chars.next().is_none()
}

fn lex(input: &str) -> Result<Vec<Token<'_>>> {
    let bytes = input.as_bytes();
    let mut at = 0;
    let mut tokens = Vec::new();
    while at < bytes.len() {
        match bytes[at] {
            c if c.is_ascii_whitespace() => {
                at += 1;
            }
            b'(' => {
                tokens.push(Token::Left);
                at += 1;
            }
            b')' => {
                tokens.push(Token::Right);
                at += 1;
            }
            b',' => {
                tokens.push(Token::Comma);
                at += 1;
            }
            b'=' | b'<' | b'>' | b'!' => {
                let begin = at;
                at += 1;
                if bytes[begin] != b'=' && at < bytes.len() && bytes[at] == b'=' {
                    at += 1;
                }
                if bytes[begin] == b'!' && at == begin + 1 {
                    return Err(invalid("invalid SOQL comparison operator"));
                }
                tokens.push(Token::Operator);
            }
            b'\'' => {
                at += 1;
                let mut closed = false;
                while at < bytes.len() {
                    match bytes[at] {
                        b'\'' => {
                            at += 1;
                            closed = true;
                            break;
                        }
                        b'\\' => {
                            at += 1;
                            if at >= bytes.len()
                                || !matches!(
                                    bytes[at],
                                    b'\''
                                        | b'"'
                                        | b'\\'
                                        | b'n'
                                        | b'r'
                                        | b't'
                                        | b'b'
                                        | b'f'
                                        | b'u'
                                        | b'%'
                                        | b'_'
                                )
                            {
                                return Err(invalid("invalid SOQL string escape"));
                            }
                            if bytes[at] == b'u' {
                                if at + 4 >= bytes.len()
                                    || !bytes[at + 1..at + 5].iter().all(u8::is_ascii_hexdigit)
                                {
                                    return Err(invalid("invalid SOQL Unicode escape"));
                                }
                                at += 4;
                            }
                            at += 1;
                        }
                        b'\n' | b'\r' | 0 => return Err(invalid("invalid SOQL string literal")),
                        _ => {
                            at += 1;
                        }
                    }
                }
                if !closed {
                    return Err(invalid("unterminated SOQL string literal"));
                }
                tokens.push(Token::Literal);
            }
            c if c.is_ascii_alphabetic() || c == b'_' => {
                let start = at;
                at += 1;
                while at < bytes.len()
                    && (bytes[at].is_ascii_alphanumeric() || matches!(bytes[at], b'_' | b'.'))
                {
                    at += 1;
                }
                let word = &input[start..at];
                if at < bytes.len() && bytes[at] == b':' {
                    at += 1;
                    let begin = at;
                    while at < bytes.len() && bytes[at].is_ascii_digit() {
                        at += 1;
                    }
                    if at == begin || !field_path(word) {
                        return Err(invalid("invalid SOQL date literal"));
                    }
                    tokens.push(Token::Literal);
                } else {
                    if !field_path(word) {
                        return Err(invalid("invalid SOQL field path"));
                    }
                    tokens.push(Token::Word(word));
                }
            }
            c if c.is_ascii_digit() || c == b'-' || c == b'+' => {
                let start = at;
                at += 1;
                while at < bytes.len()
                    && !bytes[at].is_ascii_whitespace()
                    && !matches!(bytes[at], b',' | b'(' | b')' | b'=' | b'<' | b'>' | b'!')
                {
                    at += 1;
                }
                let literal = &input[start..at];
                let date = chrono::NaiveDate::parse_from_str(literal, "%Y-%m-%d").is_ok();
                let timestamp = chrono::DateTime::parse_from_rfc3339(literal).is_ok();
                if !numeric(literal) && !date && !timestamp {
                    return Err(invalid("invalid SOQL numeric/date literal"));
                }
                tokens.push(Token::Literal);
            }
            _ => return Err(invalid("invalid token in SOQL row predicate")),
        }
    }
    Ok(tokens)
}

struct Parser<'a> {
    tokens: Vec<Token<'a>>,
    at: usize,
    depth: usize,
}
impl Parser<'_> {
    fn peek(&self) -> Option<Token<'_>> {
        self.tokens.get(self.at).copied()
    }
    fn keyword(&mut self, keyword: &str) -> bool {
        if matches!(self.peek(), Some(Token::Word(word)) if word.eq_ignore_ascii_case(keyword)) {
            self.at += 1;
            true
        } else {
            false
        }
    }
    fn expect(&mut self, token: Token<'_>) -> Result<()> {
        if self.peek() == Some(token) {
            self.at += 1;
            Ok(())
        } else {
            Err(invalid("invalid SOQL row predicate grammar"))
        }
    }
    fn expression(&mut self) -> Result<()> {
        self.conjunction()?;
        while self.keyword("OR") {
            self.conjunction()?;
        }
        Ok(())
    }
    fn conjunction(&mut self) -> Result<()> {
        self.condition()?;
        while self.keyword("AND") {
            self.condition()?;
        }
        Ok(())
    }
    fn enter(&mut self) -> Result<()> {
        self.depth += 1;
        if self.depth > 128 {
            return Err(invalid(
                "SOQL predicate nesting exceeds parser resource limit",
            ));
        }
        Ok(())
    }
    fn condition(&mut self) -> Result<()> {
        self.enter()?;
        if self.keyword("NOT") {
            self.condition()?;
        } else if self.peek() == Some(Token::Left) {
            self.at += 1;
            self.expression()?;
            self.expect(Token::Right)?;
        } else {
            self.scalar(true)?;
            if self.peek() == Some(Token::Operator) {
                self.at += 1;
                self.scalar(false)?;
            } else if self.keyword("LIKE") {
                self.scalar(false)?;
            } else {
                let negated = self.keyword("NOT");
                if !self.keyword("IN")
                    && (negated || (!self.keyword("INCLUDES") && !self.keyword("EXCLUDES")))
                {
                    return Err(invalid("SOQL field expression requires a comparison"));
                }
                self.expect(Token::Left)?;
                self.scalar(false)?;
                while self.peek() == Some(Token::Comma) {
                    self.at += 1;
                    self.scalar(false)?;
                }
                self.expect(Token::Right)?;
            }
        }
        self.depth -= 1;
        Ok(())
    }
    fn scalar(&mut self, require_field: bool) -> Result<()> {
        self.enter()?;
        match self.peek() {
            Some(Token::Word(word)) => {
                if [
                    "SELECT", "FROM", "WHERE", "ORDER", "LIMIT", "GROUP", "HAVING", "AND", "OR",
                    "NOT", "IN",
                ]
                .iter()
                .any(|reserved| word.eq_ignore_ascii_case(reserved))
                {
                    return Err(invalid("SOQL row predicate cannot contain query clauses"));
                }
                self.at += 1;
                if self.peek() == Some(Token::Left) {
                    self.at += 1;
                    if self.peek() != Some(Token::Right) {
                        self.scalar(false)?;
                        while self.peek() == Some(Token::Comma) {
                            self.at += 1;
                            self.scalar(false)?;
                        }
                    }
                    self.expect(Token::Right)?;
                }
            }
            Some(Token::Literal) if !require_field => {
                self.at += 1;
            }
            _ => return Err(invalid("invalid SOQL scalar expression")),
        }
        self.depth -= 1;
        Ok(())
    }
}

impl PredicateCompiler for SoqlPredicateCompiler {
    fn validate_row_predicate(&mut self, expression: &str) -> Result<()> {
        let mut parser = Parser {
            tokens: lex(expression)?,
            at: 0,
            depth: 0,
        };
        parser.expression()?;
        if parser.at != parser.tokens.len() {
            return Err(invalid("trailing content after SOQL row predicate"));
        }
        Ok(())
    }
}
