use crate::query_plan::QueryPlanCost;
use std::collections::VecDeque;

type Choices<T> = Vec<Option<T>>;

struct Partial<Plan, Element> {
    partial_plan: Plan,
    partial_cost: Option<QueryPlanCost>,
    remaining: std::vec::IntoIter<Choices<Element>>,
    is_root: bool,
    index: Option<usize>,
}

struct Extracted<Element> {
    extracted: Element,
    is_last: bool,
}

/// Given some initial partial plan and a list of options for the remaining parts
/// that need to be added to that plan to make it complete,
/// this method "efficiently" generates (or at least evaluate) all the possible complete plans
/// and the returns the "best" one (the one with the lowest cost).
///
/// Note that this method abstracts the actual types of both plans (type parameter `Plan`)
/// and additional elements to add to the plan (type parameter `Element`).
/// This is done for both the clarity and to make testing of this method easier.
///
/// Type parameter `Plan` should be thought of as abstracting a query plan but in practice,
/// it is instantiated to a pair of a (`DependencyGraph`, corresponding `PathTree`).
///
/// Type parameter `Element` should be thought of as an additional element
/// to add to the plan to make it complete.
/// It is instantiated in practice by a `PathTree` (for ... reasons ...)
/// that really correspond to a single `GraphPath`.
///
/// As said above, this method takes 2 arguments:
/// - `initial` is a partial plan,
///   and corresponds to all the parts of the query being planned for which there no choices
///   (and theoretically can be empty, though very very rarely is in practice).
/// - `to_add` is the list of additional elements to add to `initial`
///   to make a full plan of the query being planned.
///   Each element of `to_add` corresponds to one of the query "leaf"
///   and is itself a list of all the possible options for that "leaf".
///
/// In other words, a complete plan is obtained
/// by picking one choice in each of the element of `to_add` (so picking `to_add.len()` element)
/// and adding them all to `initial`.
/// The question being, which particular choice for each element of `to_add` yield the best plan.
///
/// Of course, the total number of possible plans is the cartesian product of `to_add`,
/// which can get large, and so this method is trying to trim some of the options.
/// For that, the general idea is that we first generate one of the plan, compute its cost,
/// and then as we build other options,
/// we can check as we pick elements of `to_add` the cost of what we get,
/// and if we ever get a higher cost than the one fo the complete plan we already have,
/// then there is no point in checking the remaining elements,
/// and we can thus cut all the options for the remaining elements.
/// In other words, if a partial plan is ever already more costly
/// than another full plan we have computed, then adding more will never get us a better plan.
///
/// Of course, this method is not guaranteed to save work,
/// and in the worst case, we'll still generate all plans.
/// But when a "good" plan is generated early, it can save a lot of computing work.
///
/// And the 2nd "trick" of this method is that it starts by generating the plans
/// that correspond to picking choices in `to_add` at the same indexes,
/// and this because this often actually generate good plans.
/// The reason is that the order of choices for each element of `to_add` is not necessarily random,
/// because the algorithm generating paths is not random either.
/// In other words, elements at similar indexes have some good chance
/// to correspond to similar choices, and so will tend to correspond to good plans.
///
/// Parameters:
/// * `initial`: the initial partial plan to use.
/// * `to_add`: a list of the remaining "elements" to add to `initial`.
///   Each element of `to_add` correspond to multiple choice
///   we can use to plan that particular element.
///   The `Option`s in side `type Choices<T> = Vec<Option<T>>` are for internal use
///   and should all be `Some` when calling this function.
/// * `add_function`: how to obtain a new plan by taking some plan and adding a new element to it.
/// * `cost_function`: how to compute the cost of a plan.
/// * `on_plan`: an optional method called on every _complete_ plan generated by this method,
///   with both the cost of that plan and the best cost we have generated thus far
///   (if that's not the first plan generated).
///   This mostly exists to allow some debugging.
pub fn generate_all_plans_and_find_best<Plan, Element>(
    initial: Plan,
    to_add: Vec<Choices<Element>>,
    mut add_function: impl FnMut(&Plan, Element) -> Plan,
    mut cost_function: impl FnMut(&Plan) -> QueryPlanCost,
    mut on_plan: impl FnMut(&Plan, QueryPlanCost, Option<QueryPlanCost>),
) -> (Plan, QueryPlanCost)
where
    Element: Clone,
    // Uncomment to use `dbg!()`
    // Element: std::fmt::Debug,
{
    if to_add.is_empty() {
        let cost = cost_function(&initial);
        return (initial, cost);
    }
    // Note: we save ourselves the computation of the cost of `initial`
    // (we pass no `partial_cost` in this initialization).
    // That's because `partial_cost` is about exiting early when we've found at least one full plan
    // and the cost of that plan is already smaller than the cost of a partial computation.
    // But any plan is going be built from `initial` with at least one 'choice' added to it,
    // all plans are guaranteed to be more costly than `initial` anyway.
    // Note that save for `initial`,
    // we always compute `partialCost` as the pros of exiting some branches early are large enough
    // that it outweight computing some costs unecessarily from time to time.
    let mut stack = VecDeque::new();
    stack.push_back(Partial {
        partial_plan: initial,
        partial_cost: None,
        remaining: to_add.into_iter(),
        is_root: true,
        index: Some(0),
    });

    let mut min = None;
    while let Some(Partial {
        partial_plan,
        partial_cost,
        mut remaining,
        is_root,
        index,
    }) = stack.pop_back()
    {
        // If we've found some plan already,
        // and the partial we have is already more costly than that,
        // then no point continuing with it.
        if let (Some((_, min_cost)), Some(partial_cost)) = (&min, &partial_cost) {
            if partial_cost >= min_cost {
                continue;
            }
        }

        // Does not panic as we only ever insert in the stack with non-empty `remaining`
        let next_choices = &mut remaining.as_mut_slice()[0];

        let picked_index = pick_next(index, next_choices);
        let Extracted { extracted, is_last } = extract(picked_index, next_choices);
        let new_partial_plan = add_function(&partial_plan, extracted);
        let cost = cost_function(&new_partial_plan);

        if !is_last {
            // First, re-insert what correspond to all the choices not in `extracted`.
            insert_in_stack(
                &mut stack,
                Partial {
                    partial_plan,
                    partial_cost,
                    is_root,
                    index: index.and_then(|i| {
                        let next = i + 1;
                        (is_root && next < next_choices.len()).then_some(next)
                    }),
                    // TODO: can we avoid cloning?
                    remaining: remaining.clone(),
                },
            )
        }
        // Done for now with `next_choices`
        remaining.next();

        let previous_min_cost = min.as_ref().map(|&(_, cost)| cost);
        let previous_min_is_better = previous_min_cost.is_some_and(|min| min <= cost);
        if remaining.as_slice().is_empty() {
            // We have a complete plan. If it is best, save it, otherwise, we're done with it.
            on_plan(&new_partial_plan, cost, previous_min_cost);
            if !previous_min_is_better {
                min = Some((new_partial_plan, cost))
            }
            continue;
        }

        if !previous_min_is_better {
            insert_in_stack(
                &mut stack,
                Partial {
                    partial_plan: new_partial_plan,
                    partial_cost: Some(cost),
                    remaining,
                    is_root: false,
                    index,
                },
            )
        }
    }
    min.expect("A plan should have been found")
}

fn insert_in_stack<Plan, Element>(
    stack: &mut VecDeque<Partial<Plan, Element>>,
    item: Partial<Plan, Element>,
) {
    // We push elements with a fixed index at the end so they are handled first.
    if item.index.is_some() {
        stack.push_back(item)
    } else {
        stack.push_front(item)
    }
}

fn pick_next<Element>(opt_index: Option<usize>, remaining: &Choices<Element>) -> usize {
    if let Some(index) = opt_index {
        if let Some(choice) = remaining.get(index) {
            assert!(choice.is_some(), "Invalid index {index}");
            return index;
        }
    }
    remaining
        .iter()
        .position(|choice| choice.is_some())
        .expect("Passed a `remaining` with all `None`")
}

fn extract<Element>(index: usize, choices: &mut Choices<Element>) -> Extracted<Element> {
    let extracted = choices[index].take().unwrap();
    let is_last = choices.iter().all(|choice| choice.is_none());
    Extracted { extracted, is_last }
}

#[cfg(test)]
mod tests {
    use super::*;

    type Element = &'static str;
    type Plan = Vec<Element>;

    /// Returns (best, generated)
    fn generate_test_plans(initial: Plan, choices: Vec<Vec<Option<Element>>>) -> (Plan, Vec<Plan>) {
        let mut generated = Vec::new();
        let target_len = initial.len() + choices.len();
        let (best, _) = generate_all_plans_and_find_best::<Plan, Element>(
            initial,
            choices,
            |partial_plan, new_element| {
                let new_plan: Plan = partial_plan
                    .iter()
                    .cloned()
                    .chain(std::iter::once(new_element))
                    .collect();
                if new_plan.len() == target_len {
                    generated.push(new_plan.clone())
                }
                new_plan
            },
            |plan| {
                plan.iter()
                    .map(|element| element.len() as QueryPlanCost)
                    .sum()
            },
            |_, _, _| {},
        );
        (best, generated)
    }

    #[test]
    fn pick_elements_at_same_index_first() {
        let (best, generated) = generate_test_plans(
            vec!["I"],
            vec![
                vec![Some("A1"), Some("B1")],
                vec![Some("A2"), Some("B2")],
                vec![Some("A3"), Some("B3")],
            ],
        );
        assert_eq!(best, ["I", "A1", "A2", "A3"]);
        assert_eq!(generated[0], ["I", "A1", "A2", "A3"]);
        assert_eq!(generated[1], ["I", "B1", "B2", "B3"]);
    }

    #[test]
    fn bail_early_for_more_costly_elements() {
        let (best, generated) = generate_test_plans(
            vec!["I"],
            vec![
                vec![Some("A1"), Some("B1VeryCostly")],
                vec![Some("A2"), Some("B2Co")],
                vec![Some("A3"), Some("B3")],
            ],
        );

        assert_eq!(best, ["I", "A1", "A2", "A3"]);
        // We should ignore plans with both B1 and B2 due there cost. So we should have just 2 plans.
        assert_eq!(generated.len(), 2);
        assert_eq!(generated[0], ["I", "A1", "A2", "A3"]);
        assert_eq!(generated[1], ["I", "A1", "A2", "B3"]);
    }

    #[test]
    fn handles_branches_of_various_sizes() {
        let (best, mut generated) = generate_test_plans(
            vec!["I"],
            vec![
                vec![Some("A1x"), Some("B1")],
                vec![Some("A2"), Some("B2Costly"), Some("C2")],
                vec![Some("A3")],
                vec![Some("A4"), Some("B4")],
            ],
        );

        assert_eq!(best, ["I", "B1", "A2", "A3", "A4"]);
        // We don't want to rely on ordering
        // (the tests ensures we get the best plan that we want, and the rest doesn't matter).
        generated.sort();
        // We should generate every option, except those including `B2Costly`
        assert_eq!(
            generated,
            [
                vec!["I", "A1x", "A2", "A3", "A4"],
                vec!["I", "A1x", "A2", "A3", "B4"],
                vec!["I", "A1x", "C2", "A3", "A4"],
                vec!["I", "A1x", "C2", "A3", "B4"],
                vec!["I", "B1", "A2", "A3", "A4"],
                vec!["I", "B1", "A2", "A3", "B4"],
                vec!["I", "B1", "C2", "A3", "A4"],
                vec!["I", "B1", "C2", "A3", "B4"],
            ],
        );
    }
}