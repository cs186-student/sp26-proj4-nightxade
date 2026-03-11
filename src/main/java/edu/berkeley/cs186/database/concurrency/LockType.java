package edu.berkeley.cs186.database.concurrency;

import java.util.*;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // DONE(proj4_part1): implement

        Map<LockType, List<LockType>> compatMap = new HashMap<>();
        compatMap.put(NL, new ArrayList<LockType>(Arrays.asList(NL, IS, IX, S, SIX, X)));
        compatMap.put(IS, new ArrayList<LockType>(Arrays.asList(NL, IS, IX, S, SIX)));
        compatMap.put(IX, new ArrayList<LockType>(Arrays.asList(NL, IS, IX)));
        compatMap.put(S, new ArrayList<LockType>(Arrays.asList(NL, IS, S)));
        compatMap.put(SIX, new ArrayList<LockType>(Arrays.asList(NL, IS)));
        compatMap.put(X, new ArrayList<LockType>(Collections.singletonList(NL)));

        return compatMap.get(a).contains(b);
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // DONE(proj4_part1): implement

        Map<LockType, List<LockType>> parentMap = new HashMap<>();
        parentMap.put(NL, new ArrayList<LockType>(Collections.singletonList(NL)));
        parentMap.put(IS, new ArrayList<LockType>(Arrays.asList(NL, IS, S)));
        parentMap.put(IX, new ArrayList<LockType>(Arrays.asList(NL, IS, IX, S, SIX, X)));
        parentMap.put(S, new ArrayList<LockType>(Arrays.asList(NL, IS)));
        parentMap.put(SIX, new ArrayList<LockType>(Arrays.asList(NL, IS, IX, S, X)));
        parentMap.put(X, new ArrayList<LockType>(Arrays.asList(NL, IS, IX, S, SIX, X)));

        return parentMap.get(parentLockType).contains(childLockType);
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // DONE(proj4_part1): implement

        Map<LockType, List<LockType>> subMap = new HashMap<>();
        subMap.put(NL, new ArrayList<LockType>(Arrays.asList(NL, IS, IX, S, SIX, X)));
        subMap.put(IS, new ArrayList<LockType>(Arrays.asList(IS, IX, S, SIX, X)));
        subMap.put(IX, new ArrayList<LockType>(Arrays.asList(IX, SIX, X)));
        subMap.put(S, new ArrayList<LockType>(Arrays.asList(S, SIX, X)));
        subMap.put(SIX, new ArrayList<LockType>(Arrays.asList(SIX, X)));
        subMap.put(X, new ArrayList<LockType>(Collections.singletonList(X)));

        return subMap.get(required).contains(substitute);
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

