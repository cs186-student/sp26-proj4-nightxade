package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // DONE(proj4_part2): implement
        if (requestType == LockType.NL)
            return;

        /* effectiveLockType bc imagine S -> NL, NL doesn't need to acquire lock */
        if (LockType.substitutable(effectiveLockType, requestType))
            return;

        if (explicitLockType == LockType.IX && requestType == LockType.S) {
            /* SIX promotion; note: SIX -> SIX already handled by substitution case */
            /* No need for ensureAncestorContext bc explicit=IX => ancestor allows S alrdy */
            lockContext.promote(transaction, LockType.SIX);
        } else if (explicitLockType.isIntent()) {
            ensureAncestorContext(lockContext, requestType);
            lockContext.escalate(transaction);
            if (lockContext.getExplicitLockType(transaction) != requestType) /* still need promotion if only IS -> S, req X */
                lockContext.promote(transaction, requestType);
        } else {
            ensureAncestorContext(lockContext, requestType);
            if (explicitLockType == LockType.NL)
                lockContext.acquire(transaction, requestType);
            else
                lockContext.promote(transaction, requestType);
        }
    }

    // DONE(proj4_part2) add any helper methods you want

    private static void ensureAncestorContext(LockContext lockContext, LockType childType) {
        TransactionContext transaction = TransactionContext.getTransaction();
        LockContext parentContext = lockContext.parentContext();
        if (parentContext == null)
            return;

        LockType parentType = parentContext.getExplicitLockType(transaction);
        LockType desiredType = LockType.parentLock(childType); /* Only IS or IX */
        if (desiredType == LockType.IX && parentType == LockType.S) /* SIX */
            desiredType = LockType.SIX;
        ensureAncestorContext(parentContext, desiredType);
        if (parentType == LockType.NL) {
            parentContext.acquire(transaction, desiredType);
        } else if (!LockType.canBeParentLock(parentType, childType)) {
            parentContext.promote(transaction, desiredType);
        }
    }
}
