package org.voltdb.hadoop.typeto;

import static org.voltdb.hadoop.TypeAide.DECIMAL;
import static org.voltdb.hadoop.TypeAide.STRING;

import java.math.BigDecimal;
import java.util.Date;
import java.util.EnumSet;

import org.voltdb.hadoop.TypeAide;
import org.voltdb.types.VoltDecimalHelper;

import com.google_voltpatches.common.base.Function;

public class BigDecimalTypeTo extends TypeTo<BigDecimal> {
    final static EnumSet<TypeAide> loose = EnumSet.allOf(TypeAide.class);
    final static EnumSet<TypeAide> strict = EnumSet.of(DECIMAL,STRING);

    public BigDecimalTypeTo(TypeAide type, boolean strictCompatibility) {
        super(type, strictCompatibility);
    }

    public BigDecimalTypeTo(TypeAide type) {
        this(type, true);
    }

    @Override
    public Function<BigDecimal, Object> getAdjuster() {
        return m_typeTo.accept(vtor, null, null);
    }

    @Override
    public Function<BigDecimal, Object> getAdjusterFor(TypeAide type) {
        return type.accept(vtor, null, null);
    }

    @Override
    public boolean isCompatibleWith(TypeAide type, boolean strictly) {
        return (strictly? strict:loose).contains(type);
    }

    private final TypeAide.Visitor<Function<BigDecimal, Object>, Void, RuntimeException> vtor =
            new TypeAide.Visitor<Function<BigDecimal,Object>, Void, RuntimeException>() {
                @Override
                public Function<BigDecimal, Object> visitVarBinary(Void p, Object v) {
                    return new Function<BigDecimal, Object>() {
                        @Override
                        final public Object apply(BigDecimal v) {
                            if (v == null) return null;
                            return VoltDecimalHelper.serializeBigDecimal(v);
                        }
                    };
                }
                @Override
                public Function<BigDecimal, Object> visitTinyInt(Void p, Object v) {
                    return new Function<BigDecimal, Object>() {
                        @Override
                        final public Object apply(BigDecimal v) {
                            if (v == null) return null;
                            return v.byteValueExact();
                        }
                    };
                }
                @Override
                public Function<BigDecimal, Object> visitTimestamp(Void p, Object v) {
                    return new Function<BigDecimal, Object>() {
                        @Override
                        final public Object apply(BigDecimal v) {
                            if (v == null) return null;
                            return new Date(v.longValueExact());
                        }
                    };
                }
                @Override
                public Function<BigDecimal, Object> visitString(Void p, Object v) {
                    return new Function<BigDecimal, Object>() {
                        @Override
                        final public Object apply(BigDecimal v) {
                            if (v == null) return null;
                            return v.toString();
                        }
                    };
                }
                @Override
                public Function<BigDecimal, Object> visitSmallInt(Void p, Object v) {
                    return new Function<BigDecimal, Object>() {
                        @Override
                        final public Object apply(BigDecimal v) {
                            if (v == null) return null;
                            return v.shortValueExact();
                        }
                    };
                }
                @Override
                public Function<BigDecimal, Object> visitInteger(Void p, Object v) {
                    return new Function<BigDecimal, Object>() {
                        @Override
                        final public Object apply(BigDecimal v) {
                            if (v == null) return null;
                            return v.intValueExact();
                        }
                    };
                }
                @Override
                public Function<BigDecimal, Object> visitFloat(Void p, Object v) {
                    return new Function<BigDecimal, Object>() {
                        @Override
                        final public Object apply(BigDecimal v) {
                            if (v == null) return null;
                            return v.doubleValue();
                        }
                    };
                }
                @Override
                public Function<BigDecimal, Object> visitDecimal(Void p, Object v) {
                    return new Function<BigDecimal, Object>() {
                        @Override
                        final public Object apply(BigDecimal v) {
                            if (v == null) return null;
                            return v;
                        }
                    };
                }
                @Override
                public Function<BigDecimal, Object> visitBigInt(Void p, Object v) {
                    return new Function<BigDecimal, Object>() {
                        @Override
                        final public Object apply(BigDecimal v) {
                            if (v == null) return null;
                            return v.longValueExact();
                        }
                    };
                }
            };
}
