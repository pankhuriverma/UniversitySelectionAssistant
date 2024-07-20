import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { UniversityDataComponent } from './university-data.component';

describe('UniversityDataComponent', () => {
  let component: UniversityDataComponent;
  let fixture: ComponentFixture<UniversityDataComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ UniversityDataComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(UniversityDataComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
