/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst

import scala.language.implicitConversions

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types._

/**
 * A very simple SQL parser.  Based loosely on:
 * https://github.com/stephentu/scala-sql-parser/blob/master/src/main/scala/parser.scala
 *
 * Limitations:
 *  - Only supports a very limited subset of SQL.
 *
 * This is currently included mostly for illustrative purposes.  Users wanting more complete support
 * for a SQL like language should checkout the HiveQL support in the sql/hive sub-project.
 */
class SqlParser extends AbstractSparkSQLParser with DataTypeParser {

  def parseExpression(input: String): Expression = {
    // Initialize the Keywords.
    initLexical
    phrase(projection)(new lexical.Scanner(input)) match {
      case Success(plan, _) => plan
      case failureOrError => sys.error(failureOrError.toString)
    }
  }

  // Keyword is a convention with AbstractSparkSQLParser, which will scan all of the `Keyword`
  // properties via reflection the class in runtime for constructing the SqlLexical object
  protected val ALL = Keyword("ALL")
  protected val AND = Keyword("AND")
  protected val APPROXIMATE = Keyword("APPROXIMATE")
  protected val AS = Keyword("AS")
  protected val ASC = Keyword("ASC")
  protected val BETWEEN = Keyword("BETWEEN")
  protected val BY = Keyword("BY")
  protected val CASE = Keyword("CASE")
  protected val CAST = Keyword("CAST")
  protected val DESC = Keyword("DESC")
  protected val DISTINCT = Keyword("DISTINCT")
  protected val ELSE = Keyword("ELSE")
  protected val END = Keyword("END")
  protected val EXCEPT = Keyword("EXCEPT")
  protected val FALSE = Keyword("FALSE")
  protected val FROM = Keyword("FROM")
  protected val FULL = Keyword("FULL")
  protected val GROUP = Keyword("GROUP")
  protected val HAVING = Keyword("HAVING")
  protected val IN = Keyword("IN")
  protected val INNER = Keyword("INNER")
  protected val INSERT = Keyword("INSERT")
  protected val INTERSECT = Keyword("INTERSECT")
  protected val INTO = Keyword("INTO")
  protected val IS = Keyword("IS")
  protected val JOIN = Keyword("JOIN")
  protected val LEFT = Keyword("LEFT")
  protected val LIKE = Keyword("LIKE")
  protected val LIMIT = Keyword("LIMIT")
  protected val NOT = Keyword("NOT")
  protected val NULL = Keyword("NULL")
  protected val ON = Keyword("ON")
  protected val OR = Keyword("OR")
  protected val ORDER = Keyword("ORDER")
  protected val SORT = Keyword("SORT")
  protected val OUTER = Keyword("OUTER")
  protected val OVERWRITE = Keyword("OVERWRITE")
  protected val REGEXP = Keyword("REGEXP")
  protected val RIGHT = Keyword("RIGHT")
  protected val RLIKE = Keyword("RLIKE")
  protected val SELECT = Keyword("SELECT")
  protected val SEMI = Keyword("SEMI")
  protected val TABLE = Keyword("TABLE")
  protected val THEN = Keyword("THEN")
  protected val TRUE = Keyword("TRUE")
  protected val UNION = Keyword("UNION")
  protected val WHEN = Keyword("WHEN")
  protected val WHERE = Keyword("WHERE")
  protected val WITH = Keyword("WITH")

  protected lazy val start: Parser[LogicalPlan] =
    start1 | insert | cte

  protected lazy val start1: Parser[LogicalPlan] =
    (select | ("(" ~> select <~ ")")) *
    ( UNION ~ ALL        ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Union(q1, q2) }
    | INTERSECT          ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Intersect(q1, q2) }
    | EXCEPT             ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Except(q1, q2)}
    | UNION ~ DISTINCT.? ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Distinct(Union(q1, q2)) }
    )

  protected lazy val select: Parser[LogicalPlan] =
    SELECT ~> DISTINCT.? ~
      repsep(projection, ",") ~
      (FROM   ~> relations).? ~
      (WHERE  ~> expression).? ~
      (GROUP  ~  BY ~> rep1sep(expression, ",")).? ~
      (HAVING ~> expression).? ~
      sortType.? ~
      (LIMIT  ~> expression).? ^^ {
        case d ~ p ~ r ~ f ~ g ~ h ~ o ~ l =>
          val base = r.getOrElse(OneRowRelation)
          val withFilter = f.map(Filter(_, base)).getOrElse(base)
          val withProjection = g
            .map(Aggregate(_, p.map(UnresolvedAlias(_)), withFilter))
            .getOrElse(Project(p.map(UnresolvedAlias(_)), withFilter))
          val withDistinct = d.map(_ => Distinct(withProjection)).getOrElse(withProjection)
          val withHaving = h.map(Filter(_, withDistinct)).getOrElse(withDistinct)
          val withOrder = o.map(_(withHaving)).getOrElse(withHaving)
          val withLimit = l.map(Limit(_, withOrder)).getOrElse(withOrder)
          withLimit
      }

  protected lazy val insert: Parser[LogicalPlan] =
    INSERT ~> (OVERWRITE ^^^ true | INTO ^^^ false) ~ (TABLE ~> relation) ~ select ^^ {
      case o ~ r ~ s => InsertIntoTable(r, Map.empty[String, Option[String]], s, o, false)
    }

  protected lazy val cte: Parser[LogicalPlan] =
    WITH ~> rep1sep(ident ~ ( AS ~ "(" ~> start1 <~ ")"), ",") ~ (start1 | insert) ^^ {
      case r ~ s => With(s, r.map({case n ~ s => (n, Subquery(n, s))}).toMap)
    }

  protected lazy val projection: Parser[Expression] =
    expression ~ (AS.? ~> ident.?) ^^ {
      case e ~ a => a.fold(e)(Alias(e, _)())
    }

  // Based very loosely on the MySQL Grammar.
  // http://dev.mysql.com/doc/refman/5.0/en/join.html
  protected lazy val relations: Parser[LogicalPlan] =
    ( relation ~ rep1("," ~> relation) ^^ {
        case r1 ~ joins => joins.foldLeft(r1) { case(lhs, r) => Join(lhs, r, Inner, None) } }
    | relation
    )

  protected lazy val relation: Parser[LogicalPlan] =
    joinedRelation | relationFactor

  protected lazy val relationFactor: Parser[LogicalPlan] =
    ( rep1sep(ident, ".") ~ (opt(AS) ~> opt(ident)) ^^ {
        case tableIdent ~ alias => UnresolvedRelation(tableIdent, alias)
      }
      | ("(" ~> start <~ ")") ~ (AS.? ~> ident) ^^ { case s ~ a => Subquery(a, s) }
    )

  protected lazy val joinedRelation: Parser[LogicalPlan] =
    relationFactor ~ rep1(joinType.? ~ (JOIN ~> relationFactor) ~ joinConditions.?) ^^ {
      case r1 ~ joins =>
        joins.foldLeft(r1) { case (lhs, jt ~ rhs ~ cond) =>
          Join(lhs, rhs, joinType = jt.getOrElse(Inner), cond)
        }
    }

  protected lazy val joinConditions: Parser[Expression] =
    ON ~> expression

  protected lazy val joinType: Parser[JoinType] =
    ( INNER           ^^^ Inner
    | LEFT  ~ SEMI    ^^^ LeftSemi
    | LEFT  ~ OUTER.? ^^^ LeftOuter
    | RIGHT ~ OUTER.? ^^^ RightOuter
    | FULL  ~ OUTER.? ^^^ FullOuter
    )

  protected lazy val sortType: Parser[LogicalPlan => LogicalPlan] =
    ( ORDER ~ BY  ~> ordering ^^ { case o => l: LogicalPlan => Sort(o, true, l) }
    | SORT ~ BY  ~> ordering ^^ { case o => l: LogicalPlan => Sort(o, false, l) }
    )

  protected lazy val ordering: Parser[Seq[SortOrder]] =
    ( rep1sep(expression ~ direction.? , ",") ^^ {
        case exps => exps.map(pair => SortOrder(pair._1, pair._2.getOrElse(Ascending)))
      }
    )

  protected lazy val direction: Parser[SortDirection] =
    ( ASC  ^^^ Ascending
    | DESC ^^^ Descending
    )

  protected lazy val expression: Parser[Expression] =
    orExpression

  protected lazy val orExpression: Parser[Expression] =
    andExpression * (OR ^^^ { (e1: Expression, e2: Expression) => Or(e1, e2) })

  protected lazy val andExpression: Parser[Expression] =
    comparisonExpression * (AND ^^^ { (e1: Expression, e2: Expression) => And(e1, e2) })

  protected lazy val comparisonExpression: Parser[Expression] =
    ( termExpression ~ ("="  ~> termExpression) ^^ { case e1 ~ e2 => EqualTo(e1, e2) }
    | termExpression ~ ("<"  ~> termExpression) ^^ { case e1 ~ e2 => LessThan(e1, e2) }
    | termExpression ~ ("<=" ~> termExpression) ^^ { case e1 ~ e2 => LessThanOrEqual(e1, e2) }
    | termExpression ~ (">"  ~> termExpression) ^^ { case e1 ~ e2 => GreaterThan(e1, e2) }
    | termExpression ~ (">=" ~> termExpression) ^^ { case e1 ~ e2 => GreaterThanOrEqual(e1, e2) }
    | termExpression ~ ("!=" ~> termExpression) ^^ { case e1 ~ e2 => Not(EqualTo(e1, e2)) }
    | termExpression ~ ("<>" ~> termExpression) ^^ { case e1 ~ e2 => Not(EqualTo(e1, e2)) }
    | termExpression ~ ("<=>" ~> termExpression) ^^ { case e1 ~ e2 => EqualNullSafe(e1, e2) }
    | termExpression ~ NOT.? ~ (BETWEEN ~> termExpression) ~ (AND ~> termExpression) ^^ {
        case e ~ not ~ el ~ eu =>
          val betweenExpr: Expression = And(GreaterThanOrEqual(e, el), LessThanOrEqual(e, eu))
          not.fold(betweenExpr)(f => Not(betweenExpr))
      }
    | termExpression ~ (RLIKE  ~> termExpression) ^^ { case e1 ~ e2 => RLike(e1, e2) }
    | termExpression ~ (REGEXP ~> termExpression) ^^ { case e1 ~ e2 => RLike(e1, e2) }
    | termExpression ~ (LIKE   ~> termExpression) ^^ { case e1 ~ e2 => Like(e1, e2) }
    | termExpression ~ (NOT ~ LIKE ~> termExpression) ^^ { case e1 ~ e2 => Not(Like(e1, e2)) }
    | termExpression ~ (IN ~ "(" ~> rep1sep(termExpression, ",")) <~ ")" ^^ {
        case e1 ~ e2 => In(e1, e2)
      }
    | termExpression ~ (NOT ~ IN ~ "(" ~> rep1sep(termExpression, ",")) <~ ")" ^^ {
        case e1 ~ e2 => Not(In(e1, e2))
      }
    | termExpression <~ IS ~ NULL ^^ { case e => IsNull(e) }
    | termExpression <~ IS ~ NOT ~ NULL ^^ { case e => IsNotNull(e) }
    | NOT ~> termExpression ^^ {e => Not(e)}
    | termExpression
    )

  protected lazy val termExpression: Parser[Expression] =
    productExpression *
      ( "+" ^^^ { (e1: Expression, e2: Expression) => Add(e1, e2) }
      | "-" ^^^ { (e1: Expression, e2: Expression) => Subtract(e1, e2) }
      )

  protected lazy val productExpression: Parser[Expression] =
    baseExpression *
      ( "*" ^^^ { (e1: Expression, e2: Expression) => Multiply(e1, e2) }
      | "/" ^^^ { (e1: Expression, e2: Expression) => Divide(e1, e2) }
      | "%" ^^^ { (e1: Expression, e2: Expression) => Remainder(e1, e2) }
      | "&" ^^^ { (e1: Expression, e2: Expression) => BitwiseAnd(e1, e2) }
      | "|" ^^^ { (e1: Expression, e2: Expression) => BitwiseOr(e1, e2) }
      | "^" ^^^ { (e1: Expression, e2: Expression) => BitwiseXor(e1, e2) }
      )

  protected lazy val function: Parser[Expression] =
    ( ident <~ ("(" ~ "*" ~ ")") ^^ { case udfName =>
      if (lexical.normalizeKeyword(udfName) == "count") {
        Count(Literal(1))
      } else {
        throw new AnalysisException(s"invalid expression $udfName(*)")
      }
    }
    | ident ~ ("(" ~> repsep(expression, ",")) <~ ")" ^^
      { case udfName ~ exprs => UnresolvedFunction(udfName, exprs) }
    | ident ~ ("(" ~ DISTINCT ~> repsep(expression, ",")) <~ ")" ^^ { case udfName ~ exprs =>
      lexical.normalizeKeyword(udfName) match {
        case "sum" => SumDistinct(exprs.head)
        case "count" => CountDistinct(exprs)
        case _ => throw new AnalysisException(s"function $udfName does not support DISTINCT")
      }
    }
    | APPROXIMATE ~> ident ~ ("(" ~ DISTINCT ~> expression <~ ")") ^^ { case udfName ~ exp =>
      if (lexical.normalizeKeyword(udfName) == "count") {
        ApproxCountDistinct(exp)
      } else {
        throw new AnalysisException(s"invalid function approximate $udfName")
      }
    }
    | APPROXIMATE ~> "(" ~> floatLit ~ ")" ~ ident ~ "(" ~ DISTINCT ~ expression <~ ")" ^^
      { case s ~ _ ~ udfName ~ _ ~ _ ~ exp =>
        if (lexical.normalizeKeyword(udfName) == "count") {
          ApproxCountDistinct(exp, s.toDouble)
        } else {
          throw new AnalysisException(s"invalid function approximate($floatLit) $udfName")
        }
      }
    | CASE ~> expression.? ~ rep1(WHEN ~> expression ~ (THEN ~> expression)) ~
        (ELSE ~> expression).? <~ END ^^ {
          case casePart ~ altPart ~ elsePart =>
            val branches = altPart.flatMap { case whenExpr ~ thenExpr =>
              Seq(whenExpr, thenExpr)
            } ++ elsePart
            casePart.map(CaseKeyWhen(_, branches)).getOrElse(CaseWhen(branches))
        }
      )

  protected lazy val cast: Parser[Expression] =
    CAST ~ "(" ~> expression ~ (AS ~> dataType) <~ ")" ^^ {
      case exp ~ t => Cast(exp, t)
    }

  protected lazy val literal: Parser[Literal] =
    ( numericLiteral
    | booleanLiteral
    | stringLit ^^ {case s => Literal.create(s, StringType) }
    | NULL ^^^ Literal.create(null, NullType)
    )

  protected lazy val booleanLiteral: Parser[Literal] =
    ( TRUE ^^^ Literal.create(true, BooleanType)
    | FALSE ^^^ Literal.create(false, BooleanType)
    )

  protected lazy val numericLiteral: Parser[Literal] =
    signedNumericLiteral | unsignedNumericLiteral

  protected lazy val sign: Parser[String] =
    "+" | "-"

  protected lazy val signedNumericLiteral: Parser[Literal] =
    ( sign ~ numericLit  ^^ { case s ~ l => Literal(toNarrowestIntegerType(s + l)) }
    | sign ~ floatLit ^^ { case s ~ f => Literal((s + f).toDouble) }
    )

  protected lazy val unsignedNumericLiteral: Parser[Literal] =
    ( numericLit ^^ { n => Literal(toNarrowestIntegerType(n)) }
    | floatLit ^^ { f => Literal(f.toDouble) }
    )

  private def toNarrowestIntegerType(value: String): Any = {
    val bigIntValue = BigDecimal(value)

    bigIntValue match {
      case v if bigIntValue.isValidInt => v.toIntExact
      case v if bigIntValue.isValidLong => v.toLongExact
      case v => v.underlying()
    }
  }

  protected lazy val floatLit: Parser[String] =
    ( "." ~> unsignedNumericLiteral ^^ { u => "0." + u }
    | elem("decimal", _.isInstanceOf[lexical.FloatLit]) ^^ (_.chars)
    )

  protected lazy val baseExpression: Parser[Expression] =
    ( "*" ^^^ UnresolvedStar(None)
    | ident <~ "." ~ "*" ^^ { case tableName => UnresolvedStar(Option(tableName)) }
    | primary
    )

  protected lazy val signedPrimary: Parser[Expression] =
    sign ~ primary ^^ { case s ~ e => if (s == "-") UnaryMinus(e) else e}

  protected lazy val primary: PackratParser[Expression] =
    ( literal
    | expression ~ ("[" ~> expression <~ "]") ^^
      { case base ~ ordinal => UnresolvedExtractValue(base, ordinal) }
    | (expression <~ ".") ~ ident ^^
      { case base ~ fieldName => UnresolvedExtractValue(base, Literal(fieldName)) }
    | cast
    | "(" ~> expression <~ ")"
    | function
    | dotExpressionHeader
    | ident ^^ {case i => UnresolvedAttribute.quoted(i)}
    | signedPrimary
    | "~" ~> expression ^^ BitwiseNot
    )

  protected lazy val dotExpressionHeader: Parser[Expression] =
    (ident <~ ".") ~ ident ~ rep("." ~> ident) ^^ {
      case i1 ~ i2 ~ rest => UnresolvedAttribute(Seq(i1, i2) ++ rest)
    }
}
