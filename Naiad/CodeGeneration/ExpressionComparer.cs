/*
 * Naiad ver. 0.2
 * Copyright (c) Microsoft Corporation
 * All rights reserved. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0 
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using System.Collections.ObjectModel;

namespace Microsoft.Research.Naiad.CodeGeneration
{

    public class ExpressionComparer
    {
        private static ExpressionComparer instance = new ExpressionComparer();
        public static ExpressionComparer Instance { get { return instance; } }

        protected ExpressionComparer()
        {
            // Potentially store some state about parameter substitutions.
        }

        public bool Equals(Expression left, Expression right)
        {
            return this.Visit(left, right);
        }
        
        protected virtual bool Visit(Expression exp, Expression other)
        {
            if (exp == null || other == null)
                return exp == null && other == null;

            if (exp.NodeType != other.NodeType)
                return false;

            switch (exp.NodeType)
            {
                case ExpressionType.Negate:
                case ExpressionType.NegateChecked:
                case ExpressionType.Not:
                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                case ExpressionType.ArrayLength:
                case ExpressionType.Quote:
                case ExpressionType.TypeAs:
                    return this.VisitUnary((UnaryExpression)exp, (UnaryExpression)other);
                case ExpressionType.Add:
                case ExpressionType.AddChecked:
                case ExpressionType.Subtract:
                case ExpressionType.SubtractChecked:
                case ExpressionType.Multiply:
                case ExpressionType.MultiplyChecked:
                case ExpressionType.Divide:
                case ExpressionType.Modulo:
                case ExpressionType.And:
                case ExpressionType.AndAlso:
                case ExpressionType.Or:
                case ExpressionType.OrElse:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                case ExpressionType.Coalesce:
                case ExpressionType.ArrayIndex:
                case ExpressionType.RightShift:
                case ExpressionType.LeftShift:
                case ExpressionType.ExclusiveOr:
                    return this.VisitBinary((BinaryExpression)exp, (BinaryExpression)other);
                case ExpressionType.TypeIs:
                    return this.VisitTypeIs((TypeBinaryExpression)exp, (TypeBinaryExpression)other);
                case ExpressionType.Conditional:
                    return this.VisitConditional((ConditionalExpression)exp, (ConditionalExpression)other);
                case ExpressionType.Constant:
                    return this.VisitConstant((ConstantExpression)exp, (ConstantExpression)other);
                case ExpressionType.Parameter:
                    return this.VisitParameter((ParameterExpression)exp, (ParameterExpression)other);
                case ExpressionType.MemberAccess:
                    return this.VisitMemberAccess((MemberExpression)exp, (MemberExpression)other);
                case ExpressionType.Call:
                    return this.VisitMethodCall((MethodCallExpression)exp, (MethodCallExpression)other);
                case ExpressionType.Lambda:
                    return this.VisitLambda((LambdaExpression)exp, (LambdaExpression)other);
                case ExpressionType.New:
                    return this.VisitNew((NewExpression)exp, (NewExpression)other);
                case ExpressionType.NewArrayInit:
                case ExpressionType.NewArrayBounds:
                    return this.VisitNewArray((NewArrayExpression)exp, (NewArrayExpression)other);
                case ExpressionType.Invoke:
                    return this.VisitInvocation((InvocationExpression)exp, (InvocationExpression)other);
                case ExpressionType.MemberInit:
                    return this.VisitMemberInit((MemberInitExpression)exp, (MemberInitExpression)other);
                case ExpressionType.ListInit:
                    return this.VisitListInit((ListInitExpression)exp, (ListInitExpression)other);
                default:
                    throw new Exception(string.Format("Unhandled expression type: '{0}'", exp.NodeType));
            }
        }

        protected virtual bool VisitBinding(MemberBinding binding, MemberBinding other)
        {
            if (binding.BindingType != other.BindingType)
                return false;
            switch (binding.BindingType)
            {
                case MemberBindingType.Assignment:
                    return this.VisitMemberAssignment((MemberAssignment)binding, (MemberAssignment)other);
                case MemberBindingType.MemberBinding:
                    return this.VisitMemberMemberBinding((MemberMemberBinding)binding, (MemberMemberBinding)other);
                case MemberBindingType.ListBinding:
                    return this.VisitMemberListBinding((MemberListBinding)binding, (MemberListBinding)other);
                default:
                    throw new Exception(string.Format("Unhandled binding type '{0}'", binding.BindingType));
            }
        }

        protected virtual bool VisitElementInitializer(ElementInit initializer, ElementInit other)
        {
            return this.VisitExpressionList(initializer.Arguments, other.Arguments);
        }

        protected virtual bool VisitUnary(UnaryExpression u, UnaryExpression other)
        {
            return this.Visit(u.Operand, other.Operand);
        }

        protected virtual bool VisitBinary(BinaryExpression b, BinaryExpression other)
        {
            return this.Visit(b.Left, other.Left) && this.Visit(b.Right, other.Right) && this.Visit(b.Conversion, other.Conversion);
        }

        protected virtual bool VisitTypeIs(TypeBinaryExpression b, TypeBinaryExpression other)
        {
            return b.TypeOperand.Equals(other.TypeOperand) && this.Visit(b.Expression, other.Expression);
        }

        protected virtual bool VisitConstant(ConstantExpression c, ConstantExpression other)
        {
            return c.Value.Equals(other.Value);
        }

        protected virtual bool VisitConditional(ConditionalExpression c, ConditionalExpression other)
        {
            return this.Visit(c.Test, other.Test) && this.Visit(c.IfTrue, other.IfTrue) && this.Visit(c.IfFalse, other.IfFalse);
        }

        protected virtual bool VisitParameter(ParameterExpression p, ParameterExpression other)
        {
            return p.Name.Equals(other.Name);
        }

        protected virtual bool VisitMemberAccess(MemberExpression m, MemberExpression other)
        {
            return this.Visit(m.Expression, other.Expression) && m.Member.Equals(other.Member);
        }

        protected virtual bool VisitMethodCall(MethodCallExpression m, MethodCallExpression other)
        {
            return this.Visit(m.Object, other.Object) && m.Method.Equals(other.Method) && this.VisitExpressionList(m.Arguments, other.Arguments);
        }

        protected virtual bool VisitExpressionList(ReadOnlyCollection<Expression> original, ReadOnlyCollection<Expression> other)
        {
            return original.Zip(other, (x, y) => this.Visit(x, y)).All(x => x);
        }

        protected virtual bool VisitExpressionList(ReadOnlyCollection<ParameterExpression> original, ReadOnlyCollection<ParameterExpression> other)
        {
            return original.Zip(other, (x, y) => this.Visit(x, y)).All(x => x);
        }

        protected virtual bool VisitMemberAssignment(MemberAssignment assignment, MemberAssignment other)
        {
            return this.Visit(assignment.Expression, other.Expression) && assignment.Member.Equals(other.Member);
        }

        protected virtual bool VisitMemberMemberBinding(MemberMemberBinding binding, MemberMemberBinding other)
        {
            return this.VisitBindingList(binding.Bindings, other.Bindings) && binding.Member.Equals(other.Member);
        }

        protected virtual bool VisitMemberListBinding(MemberListBinding binding, MemberListBinding other)
        {
            return this.VisitElementInitializerList(binding.Initializers, other.Initializers) && binding.Member.Equals(other.Member);
        }

        protected virtual bool VisitBindingList(ReadOnlyCollection<MemberBinding> original, ReadOnlyCollection<MemberBinding> other)
        {
            return original.Zip(other, (x, y) => this.VisitBinding(x, y)).All(x => x);
        }

        protected virtual bool VisitElementInitializerList(ReadOnlyCollection<ElementInit> original, ReadOnlyCollection<ElementInit> other)
        {
            return original.Zip(other, (x, y) => this.VisitElementInitializer(x, y)).All(x => x);
        }

        protected virtual bool VisitLambda(LambdaExpression lambda, LambdaExpression other)
        {
            return this.Visit(lambda.Body, other.Body) && lambda.Type.Equals(other.Type) && this.VisitExpressionList(lambda.Parameters, other.Parameters);
        }

        protected virtual bool VisitNew(NewExpression nex, NewExpression other)
        {
            return this.VisitExpressionList(nex.Arguments, other.Arguments) && nex.Constructor.Equals(other.Constructor) &&
                ((nex.Members == null && other.Members == null) || nex.Members.Zip(other.Members, (x, y) => x.Equals(y)).All(x => x));
        }

        protected virtual bool VisitMemberInit(MemberInitExpression init, MemberInitExpression other)
        {
            return this.VisitNew(init.NewExpression, other.NewExpression) && this.VisitBindingList(init.Bindings, other.Bindings);
        }

        protected virtual bool VisitListInit(ListInitExpression init, ListInitExpression other)
        {
            return this.VisitNew(init.NewExpression, other.NewExpression) && this.VisitElementInitializerList(init.Initializers, other.Initializers);
        }

        protected virtual bool VisitNewArray(NewArrayExpression na, NewArrayExpression other)
        {
            return this.VisitExpressionList(na.Expressions, other.Expressions) && na.Type.GetElementType().Equals(na.Type.GetElementType());
        }

        protected virtual bool VisitInvocation(InvocationExpression iv, InvocationExpression other)
        {
            return this.VisitExpressionList(iv.Arguments, other.Arguments) && this.Visit(iv.Expression, other.Expression);
        }
    }



}
