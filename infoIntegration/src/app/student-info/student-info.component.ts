import { Component, OnInit } from '@angular/core';
import { HomeService } from '../home.service';
import { SortEvent } from 'primeng/api';

@Component({
  selector: 'app-student-info',
  templateUrl: './student-info.component.html',
  styleUrls: ['./student-info.component.css']
})
export class StudentInfoComponent implements OnInit {

  constructor(private homeService: HomeService) { }
  students = [];
  ngOnInit(): void {

    this.homeService.getUniversityData().subscribe(response => {
      console.log(response);
      this.students = response;
    });
  }
}
