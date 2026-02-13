import { test } from 'vitest';
import { createElement } from '../jsx-runtime.js';
import { Pipeline } from '../../components/pipeline.js';
import { Filter } from '../../components/transforms.js';

// Intrinsic element rejection — lowercase HTML tags must produce type errors

test('div is rejected', () => {
  // @ts-expect-error — <div> does not exist on JSX.IntrinsicElements
  <div />;
});

test('span is rejected', () => {
  // @ts-expect-error — <span> does not exist on JSX.IntrinsicElements
  <span className="x" />;
});

test('table is rejected', () => {
  // @ts-expect-error — <table> does not exist on JSX.IntrinsicElements
  <table />;
});

// Valid uppercase components compile without error

test('Pipeline with required props compiles', () => {
  <Pipeline name="test" />;
});

test('Filter with required props compiles', () => {
  <Filter condition="x > 0" />;
});

// Missing required props produce type errors

test('Pipeline without name is rejected', () => {
  // @ts-expect-error — Property 'name' is missing
  <Pipeline />;
});

test('Filter without condition is rejected', () => {
  // @ts-expect-error — Property 'condition' is missing
  <Filter />;
});
