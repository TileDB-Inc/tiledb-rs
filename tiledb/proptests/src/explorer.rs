/// This is a small utility to see if there's a possible approach to replace
/// proptest's builtin binary search approach to shrinking. Having played with
/// the existing approach and not being able to successfully map it to anything
/// with even vaguely complicated structure and constraints, I've come to the
/// conclusion that binary searching probably isn't a rich enough paradigm
/// for reducing test complexity.
///
/// Rather than re-hash how proptest works and is implemented, I'll instead
/// jump to how I see this working as a library for handling test generation
/// along with some possible future extensions to improve test coverage and
/// certainty of correctness.
///
/// The first step of any property based testing is to first find a failing
/// example input to some system. I am currently a fan how I have things
/// structured with the RNG-with-State approach to generating example inputs.
/// The basic idea here is that we can generate test data by passing down an
/// Rng instance coupled with some state that informs us what possibilities
/// are able to be generated. This seems to work fairly well and is finding
/// some decently dark corners. Its also finding a lot of the same issues as
/// a more traditional proptest implementation which seems like good evidence
/// that this is a viable approach.
///
/// However, the "traditional" proptest approach has a terrible time attempting
/// to simplify input using binary search. Given a non-trivial input where the
/// basic question of "What is half as complex as the current state?" is almost
/// impossible to answer. With so many constraints its hard to say what that
/// state might be. Or which of many states that might be.
///
/// This module is exploring a completely different approach based on searching
/// through a graph of states looking for something simpler than the generated
/// failure that triggers the same error condition.
///
/// The basic idea of this approach is that given a failing test case, we
/// should be able to generate some "base" or "most simple" case that could
/// lead us to the eventual error state. This search is carried out as a
/// depth first search of the state space starting at the base case and then
/// iteratively attempting to perform a depth first search through the state
/// space to end up at the same error detected in the failing test case. This
/// approach has two main prospective benefits. First, exploring
/// multi-dimensional state space is a lot easier than attempting to mapping
/// to a single dimension complexity axis. Secondly, we should be able to
/// handle other errors with ease by abandoning branches of the search
/// entirely.
///
/// While that all sounds lovely and awesome, the kicker is how would we write
/// that as a generic API that isn't complicated as all get out? My current
/// approach here is to have an idea of an abstract set of "child states" that
/// we can try exploring independently along with a small set of signaling
/// errors that will direct the graph exploration.
///
/// I'm thinking that child states will be represented by a custom type that
/// will generally be enum variants (though users can do what ever with some
/// set of required traits). Errors will be an Error type defined by this
/// module for use in signally "go deeper into the cave" or "abandon all hope
/// ye who enter here". All of this logic will then get mapped into the current
/// proptest Strategy so that this becomes more of a proptest plugin than its
/// full on testing implementation.
///
/// One final note on future improvements to correctness that might be handy
/// is that theoretically with this state space we should be able to have a
/// optional behavior that probes error states as well. For instance, we
/// recently had a case where we wanted an FFI API to be the arbiter of
/// correctness which we use to verify our Rust API bindings are exactly
/// correct as opposed only to "only allows correct things". The difference
/// here being that we're purposefully providing "error" inputs to the APIs
/// to ensure that it accepts both what we think it does and also does *not*
/// accept what we think it doesn't.
///
/// The idea with error probing would be to extend that so that eventually
/// as our proptest corpus stabilizes we'll be able to instead switching from
/// the existing "search for correct behavior" during nightly soak tests to
/// something like "search exact correctness" where we can sample successful
/// inputs and then attempt to use this error state space search thing to
/// check that all of the things we think are errors are actually errors.
use std::fmt::Debug;

use proptest::test_runner::TestCaseError;

pub trait ValueTreeExplorer {
    type Value: Debug;

    /// Get the root case from the failing example. This should be the simplest
    /// representation of the failing test case. If the result of this call
    /// fails the test, exploration is aborted.
    fn root(&self) -> Self::Value;

    /// Get the current value of the explored state to check against the
    /// test. Each time value returned from this method passes the test case,
    /// the `explore` method is called to search further for the original
    /// error.
    ///
    /// If the test case fails with this input and the error matches the
    /// original error for the failing test, we call the `refine()` method
    /// to allow for efficiently searching for the error cause.
    ///
    /// If the test fails with any other error we abort this branch of the
    /// exploration.
    fn current(&self) -> Self::Value;

    /// Explore some new state in the search space to check and see if we can
    /// discover the error. The previous (if any) test case has succeeded so
    /// here we can either generate the next state to test or delegate further
    /// into a tree of explorers.
    ///
    /// The return value is a bit wonky with the Result<Option<T>>. Returning
    /// Ok(None) means that current() will be called on the self again. An
    /// Ok(impl ValueTreeExplorer) indicates that we want to pop a delegate
    /// explorer implementation onto the current stack.
    ///
    /// Any error returned causes exploration of this branch to be abandoned.
    fn explore(
        &mut self,
    ) -> Result<
        Option<impl ValueTreeExplorer<Value = Self::Value>>,
        TestCaseError,
    >;

    /// If a value returned by `current()` fails the test case, this method is
    /// called to check if the case can be refined further to allow for
    /// implementations to efficiently search through their error state space.
    ///
    /// If no refinement is possible, this method should return false to
    /// indicate that the search should terminate. If this method returns true
    /// then `current()` is called again to test the refined value.
    ///
    /// If the refined value no longer fails the test, explore() is invoked.
    /// Otherwise the refine -> current -> test loop runs until this method
    /// returns false.
    fn refine(&mut self) -> bool;
}
